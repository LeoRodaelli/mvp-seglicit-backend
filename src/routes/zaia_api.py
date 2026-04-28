# -*- coding: utf-8 -*-
"""
API de Integração com o Agente de IA Zaia — v2
===============================================
Versão 2: Preferências (estados e áreas) lidas diretamente da tabela
`subscriptions` (selected_states e selected_areas), que é criada após
o pagamento aprovado no Mercado Pago.

Filtragem por área usa dicionário expandido de palavras-chave por categoria,
garantindo que licitações relacionadas sejam encontradas mesmo sem conter
o nome exato da área no objeto/título.

Endpoints disponíveis:
  GET  /api/zaia/ping                - Verificar status (público)
  POST /api/zaia/gerar-api-key       - Gera API Key para o usuário
  GET  /api/zaia/perfil              - Perfil + preferências da subscription
  GET  /api/zaia/licitacoes          - Busca com filtros (usa subscription automaticamente)
  GET  /api/zaia/licitacoes/<id>     - Detalhes completos de uma licitação
  POST /api/zaia/configurar-webhook  - Salva URL do webhook da Zaia

Como registrar no main.py:
  from src.routes.zaia_api import zaia_bp
  app.register_blueprint(zaia_bp, url_prefix='/api')
"""

from flask import Blueprint, request, jsonify
import psycopg2
import psycopg2.extras
import logging
import json
import os
import secrets
import requests as http_requests
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

zaia_bp = Blueprint('zaia', __name__)


# ============================================================
# DICIONÁRIO DE PALAVRAS-CHAVE POR ÁREA DE ATUAÇÃO
# Garante que licitações relacionadas sejam encontradas mesmo
# sem conter o nome exato da área no título/objeto.
# ============================================================

KEYWORDS_POR_AREA = {

    "Construção Civil": [
        "construção", "construcao", "obra", "obras", "reforma", "reformas",
        "edificação", "edificacao", "edificações", "edificacoes",
        "engenharia", "arquitetura", "projeto executivo",
        "pavimentação", "pavimentacao", "pavimento", "asfalto", "asfáltica",
        "asfaltica", "recapeamento", "recape", "calçada", "calcada",
        "calçamento", "calcamento", "drenagem", "esgoto", "saneamento",
        "rede de água", "rede de agua", "abastecimento de água",
        "abastecimento de agua", "galeria", "galerias pluviais",
        "contenção", "contencao", "muro de arrimo", "muro de contenção",
        "talude", "aterro", "terraplanagem", "terraplenagem",
        "ampliação", "ampliacao", "revitalização", "revitalizacao",
        "restauração", "restauracao", "recuperação", "recuperacao",
        "cimento", "concreto", "argamassa", "tijolo", "bloco de concreto",
        "areia", "brita", "ferro", "aço estrutural", "aco estrutural",
        "telha", "telhado", "cobertura", "impermeabilização", "impermeabilizacao",
        "pintura predial", "revestimento", "piso", "azulejo", "cerâmica", "ceramica",
        "esquadria", "porta", "janela", "vidro", "alumínio", "aluminio",
        "instalação elétrica", "instalacao eletrica", "instalação hidráulica",
        "instalacao hidraulica", "instalação sanitária", "instalacao sanitaria",
        "ar condicionado", "climatização", "climatizacao", "elevador",
        "empreitada", "empreitada global", "empreitada integral",
        "regime de empreitada", "execução de obra", "execucao de obra",
        "manutenção predial", "manutencao predial", "conservação predial",
        "conservacao predial", "serviços de engenharia", "servicos de engenharia",
        "consultoria de engenharia", "fiscalização de obra", "fiscalizacao de obra",
        "gerenciamento de obra", "supervisão de obra", "supervisao de obra",
        "topografia", "sondagem", "geotécnica", "geotecnica",
        "iluminação pública", "iluminacao publica", "poste", "posteamento",
        "praça", "praca", "parque", "jardim", "área de lazer", "area de lazer",
        "campo de futebol", "quadra esportiva", "pista de caminhada",
        "ciclovia", "ciclovias", "calçadão", "calcadao",
        "cbuq", "bgtc", "tsd", "rdc", "regime diferenciado",
        "contratação integrada", "construção de escola", "construção de ubs",
        "construção de creche", "construção de posto", "construção de ginásio",
        "construção de quadra", "construção de praça", "construção de parque",
        "construção de ponte", "construção de viaduto", "construção de passarela",
    ],

    "Saúde e Medicamentos": [
        "medicamento", "medicamentos", "fármaco", "farmaco", "farmácia",
        "farmacia", "remédio", "remedio", "droga", "drogas",
        "antibiótico", "antibiotico", "analgésico", "vacina", "vacinas",
        "imunobiológico", "imunobiologico", "insulina", "soro",
        "solução injetável", "solucao injetavel", "comprimido", "cápsula",
        "capsula", "ampola", "frasco", "insumo farmacêutico",
        "insumo farmaceutico", "atenção farmacêutica", "atencao farmaceutica",
        "dispensação", "dispensacao", "componente especializado",
        "componente básico", "componente basico",
        "farmácia básica", "farmacia basica",
        "equipamento médico", "equipamento medico",
        "equipamento hospitalar", "equipamentos hospitalares",
        "equipamento odontológico", "equipamento odontologico",
        "aparelho médico", "aparelho medico",
        "bisturi", "seringa", "agulha", "cateter", "sonda",
        "luva cirúrgica", "luva cirurgica", "máscara cirúrgica",
        "mascara cirurgica", "gaze", "curativo", "bandagem",
        "maca", "cadeira de rodas", "muleta", "andador", "órtese",
        "ortese", "prótese", "protese",
        "ventilador pulmonar", "respirador", "oxímetro", "oximetro",
        "desfibrilador", "eletrocardiógrafo", "eletrocardiografo",
        "monitor cardíaco", "monitor cardiaco", "ultrassom",
        "tomógrafo", "tomografo", "raio-x", "raio x",
        "mamógrafo", "mamografo", "endoscópio", "endoscopio",
        "autoclave", "esterilizador", "centrifuga", "microscópio",
        "microscopio", "analisador", "hematologia",
        "serviço de saúde", "servico de saude", "assistência médica",
        "assistencia medica", "assistência hospitalar", "assistencia hospitalar",
        "plano de saúde", "plano de saude",
        "ubs", "unidade básica de saúde", "unidade basica de saude",
        "upa", "unidade de pronto atendimento", "hospital",
        "clínica", "clinica", "ambulatório", "ambulatorio",
        "pronto-socorro", "pronto socorro", "cti", "uti",
        "hemodiálise", "hemodialise", "quimioterapia", "radioterapia",
        "fisioterapia", "fonoaudiologia", "psicologia", "nutrição",
        "nutricao", "odontologia", "oftalmologia",
        "saúde mental", "saude mental", "caps", "cras", "creas",
        "vigilância sanitária", "vigilancia sanitaria",
        "vigilância epidemiológica", "vigilancia epidemiologica",
        "samu", "resgate", "ambulância", "ambulancia",
        "material hospitalar", "material médico-hospitalar",
        "material medico hospitalar", "descartável hospitalar",
        "descartavel hospitalar", "epi hospitalar",
        "álcool gel", "alcool gel", "desinfetante hospitalar",
        "sus", "anvisa", "cnes", "sigtap", "rename",
    ],

    "Educação e Ensino": [
        "escola", "escolas", "colégio", "colegio", "creche", "creches",
        "pré-escola", "pre-escola", "educação infantil", "educacao infantil",
        "ensino fundamental", "ensino médio", "ensino medio",
        "ensino superior", "universidade", "faculdade", "instituto federal",
        "ifsp", "ifmg", "ufmg", "usp", "unicamp",
        "centro educacional", "centro de ensino",
        "material escolar", "material didático", "material didatico",
        "livro didático", "livro didatico", "livro", "livros",
        "apostila", "caderno", "mochila", "estojo", "lápis", "lapis",
        "caneta", "borracha", "régua", "regua", "compasso", "transferidor",
        "papel", "resma", "cartolina", "tinta", "pincel",
        "kit escolar", "kit pedagógico", "kit pedagogico",
        "uniforme escolar", "fardamento escolar",
        "mobiliário escolar", "mobiliario escolar",
        "carteira escolar", "mesa escolar", "cadeira escolar",
        "lousa", "quadro negro", "quadro branco", "quadro interativo",
        "armário escolar", "armario escolar", "estante", "biblioteca",
        "bebedouro escolar", "refeitório", "refeitorio",
        "equipamento educacional", "laboratório", "laboratorio",
        "computador para escola", "tablet educacional",
        "projetor", "datashow", "tela de projeção", "tela de projecao",
        "merenda escolar", "alimentação escolar", "alimentacao escolar",
        "pnae", "programa nacional de alimentação escolar",
        "transporte escolar", "ônibus escolar", "onibus escolar",
        "van escolar", "frota escolar",
        "gestão escolar", "gestao escolar", "sistema de gestão escolar",
        "plataforma educacional", "ead", "ensino a distância",
        "ensino a distancia", "ensino remoto",
        "capacitação", "capacitacao", "treinamento", "curso",
        "formação", "formacao", "qualificação", "qualificacao",
        "fnde", "pdde", "pnld", "pronatec", "prouni", "fies",
    ],

    "Tecnologia e TI": [
        "computador", "computadores", "desktop", "notebook", "laptop",
        "servidor", "servidores", "workstation", "estação de trabalho",
        "tablet", "monitor", "teclado", "mouse", "impressora",
        "multifuncional", "scanner", "nobreak", "ups", "switch",
        "roteador", "firewall", "access point", "wi-fi", "wifi",
        "rack", "storage", "hd externo", "pen drive", "memória",
        "memória ram", "processador", "placa de rede",
        "câmera ip", "camera ip", "câmera de segurança",
        "camera de segurança", "cftv", "dvr", "nvr",
        "projetor", "datashow", "televisão", "televisao", "tv",
        "painel de led", "totem", "totens", "quiosque digital",
        "software", "licença de software", "licenca de software",
        "sistema", "sistema de informação", "sistema de informacao",
        "sistema de gestão", "sistema de gestao", "erp", "crm",
        "sistema integrado", "plataforma digital", "aplicativo",
        "aplicativo móvel", "aplicativo movel", "app",
        "desenvolvimento de software", "desenvolvimento de sistema",
        "desenvolvimento de aplicativo", "programação", "programacao",
        "banco de dados", "data warehouse", "bi", "business intelligence",
        "inteligência artificial", "inteligencia artificial", "ia", "ml",
        "machine learning", "automação", "automacao", "rpa",
        "microsoft", "windows", "office", "microsoft 365",
        "google workspace", "adobe", "autocad", "antivírus", "antivirus",
        "infraestrutura de ti", "datacenter", "data center",
        "cloud", "nuvem", "computação em nuvem", "computacao em nuvem",
        "aws", "azure", "google cloud", "hosting", "hospedagem",
        "backup", "disaster recovery", "segurança da informação",
        "seguranca da informacao", "cibersegurança", "ciberseguranca",
        "pentest", "vulnerabilidade", "lgpd",
        "telecomunicação", "telecomunicacao", "telecomunicações",
        "telefonia", "telefone", "pabx", "voip", "link de internet",
        "internet", "banda larga", "fibra óptica", "fibra optica",
        "rede lógica", "rede logica", "rede de dados", "cabeamento",
        "cabeamento estruturado", "fibra", "radiofrequência",
        "radiofrequencia", "rádio", "radio", "comunicação", "comunicacao",
        "suporte técnico", "suporte tecnico", "helpdesk", "help desk",
        "manutenção de equipamentos", "manutencao de equipamentos",
        "assistência técnica", "assistencia tecnica",
        "consultoria de ti", "consultoria em tecnologia",
        "gestão de ti", "gestao de ti", "outsourcing de ti",
        "locação de equipamentos", "locacao de equipamentos",
        "impressão gerenciada", "impressao gerenciada",
        "ti", "tic", "cgti", "stic", "serpro", "dataprev",
        "sei", "sistema eletrônico de informações",
        "portal de transparência", "portal de transparencia",
        "e-gov", "governo digital", "transformação digital",
        "transformacao digital",
    ],

    "Transporte e Logística": [
        "veículo", "veiculo", "veículos", "veiculos",
        "automóvel", "automovel", "carro", "carros",
        "caminhão", "caminhao", "caminhões", "caminhoes",
        "ônibus", "onibus", "micro-ônibus", "micro-onibus", "microônibus",
        "van", "vans", "kombi", "minivan",
        "moto", "motocicleta", "motocicletas",
        "trator", "retroescavadeira", "escavadeira", "pá carregadeira",
        "pa carregadeira", "motoniveladora", "rolo compactador",
        "caminhão pipa", "caminhao pipa", "caminhão basculante",
        "caminhao basculante", "caminhão caçamba", "caminhao cacamba",
        "ambulância", "ambulancia", "viatura policial",
        "veículo oficial", "veiculo oficial", "frota",
        "locação de veículos", "locacao de veiculos",
        "locação de frota", "locacao de frota",
        "combustível", "combustivel", "combustíveis", "combustiveis",
        "gasolina", "etanol", "diesel", "óleo diesel", "oleo diesel",
        "gás natural veicular", "gas natural veicular", "gnv",
        "arla", "arla 32", "lubrificante", "lubrificantes",
        "posto de combustível", "posto de combustivel",
        "abastecimento", "abastecimento de frota",
        "manutenção de veículos", "manutencao de veiculos",
        "manutenção de frota", "manutencao de frota",
        "revisão de veículos", "revisao de veiculos",
        "pneu", "pneus", "peça automotiva", "pecas automotivas",
        "funilaria", "pintura automotiva", "mecânica", "mecanica",
        "elétrica automotiva", "eletrica automotiva",
        "logística", "logistica", "frete", "transporte de carga",
        "transporte de mercadoria", "entrega", "distribuição",
        "distribuicao", "armazenagem", "armazém", "armazem",
        "depósito", "deposito", "estoque", "almoxarifado",
        "coleta", "coleta seletiva", "coleta de resíduos",
        "coleta de residuos", "limpeza urbana",
        "transporte de passageiros", "transporte público",
        "transporte publico", "transporte coletivo",
        "transporte escolar", "transporte de pacientes",
        "transporte de servidores",
        "estrada", "rodovia", "via", "vias", "pista", "pistas",
        "sinalização viária", "sinalizacao viaria",
        "sinalização horizontal", "sinalização vertical",
        "semáforo", "semaforo", "guarda-corpo", "guard rail",
        "ponte", "viaduto", "passarela", "túnel", "tunel",
        "porto", "aeroporto", "terminal", "terminal rodoviário",
        "terminal rodoviario",
        "dnit", "der", "denatran", "detran", "antt", "antaq", "anac",
    ],

    "Serviços Gerais": [
        "limpeza", "limpeza predial", "limpeza urbana",
        "limpeza de vias", "limpeza de praças", "limpeza de repartição",
        "conservação", "conservacao", "conservação predial",
        "conservacao predial", "zeladoria", "portaria", "porteiro",
        "recepção", "recepcao", "recepcionista",
        "copeiragem", "copeira", "copa", "cozinha",
        "asseio", "asseio e conservação", "asseio e conservacao",
        "higienização", "higienizacao",
        "desinsetização", "desinsetizacao", "dedetização",
        "dedetizacao", "desratização", "desratizacao",
        "controle de pragas", "controle de vetores",
        "lavanderia", "lavagem", "passadoria",
        "coleta de lixo", "coleta de resíduos", "coleta de residuos",
        "destinação de resíduos", "destinacao de residuos",
        "aterro sanitário", "aterro sanitario",
        "segurança", "seguranca", "vigilância", "vigilancia",
        "vigilante", "vigilantes", "guarda", "guardas",
        "segurança patrimonial", "seguranca patrimonial",
        "monitoramento", "monitoramento eletrônico",
        "monitoramento eletronico", "alarme", "controle de acesso",
        "catracas", "catraca", "portão eletrônico", "portao eletronico",
        "cerca elétrica", "cerca eletrica",
        "manutenção", "manutencao", "manutenção predial",
        "manutencao predial", "manutenção preventiva",
        "manutencao preventiva", "manutenção corretiva",
        "manutencao corretiva", "manutenção de ar condicionado",
        "manutencao de ar condicionado", "manutenção de elevador",
        "manutencao de elevador", "manutenção elétrica",
        "manutencao eletrica", "manutenção hidráulica",
        "manutencao hidraulica", "manutenção de jardim",
        "manutencao de jardim", "jardinagem", "poda",
        "poda de árvores", "poda de arvores",
        "alimentação", "alimentacao", "refeição", "refeicao",
        "refeições", "refeicoes", "refeitório", "refeitorio",
        "restaurante", "cantina", "lanchonete",
        "fornecimento de refeições", "fornecimento de refeicoes",
        "coffee break", "coffee-break", "buffet",
        "gêneros alimentícios", "generos alimenticios",
        "alimento", "alimentos", "merenda", "cesta básica",
        "cesta basica", "kit alimentação", "kit alimentacao",
        "impressão", "impressao", "reprografia", "cópia", "copia",
        "plotagem", "encadernação", "encadernacao",
        "serviço gráfico", "servico grafico", "gráfica", "grafica",
        "evento", "eventos", "organização de eventos",
        "organizacao de eventos", "cerimonial", "decoração",
        "decoracao", "sonorização", "sonorizacao",
        "locação de espaço", "locacao de espaco",
        "aluguel", "locação", "locacao",
        "serviço administrativo", "servico administrativo",
        "assessoria", "consultoria", "auditoria",
        "contabilidade", "contábil", "contabil",
        "recursos humanos", "rh", "folha de pagamento",
        "medicina do trabalho", "saúde ocupacional",
        "saude ocupacional", "cipa", "ppra", "pcmso",
        "treinamento", "capacitação", "capacitacao",
        "comunicação", "comunicacao", "publicidade", "propaganda",
        "marketing", "mídia", "midia", "outdoor", "banner",
        "material gráfico", "material grafico", "panfleto",
        "folder", "cartaz", "identidade visual",
        "serviço postal", "servico postal", "correios",
        "serviço de tradução", "servico de traducao",
        "serviço jurídico", "servico juridico", "advocacia",
        "serviço técnico especializado", "servico tecnico especializado",
    ],
}


def get_keywords_para_areas(areas_lista):
    """
    Dado uma lista de nomes de áreas (ex: ["Construção Civil", "Saúde e Medicamentos"]),
    retorna a lista unificada de todas as palavras-chave relacionadas.
    Remove duplicatas e normaliza para lowercase.
    """
    keywords = set()
    for area in areas_lista:
        # Busca exata pelo nome da área
        if area in KEYWORDS_POR_AREA:
            for kw in KEYWORDS_POR_AREA[area]:
                keywords.add(kw.lower())
        else:
            # Fallback: tenta match parcial (ex: "construção" bate em "Construção Civil")
            area_lower = area.lower()
            for area_key, kw_list in KEYWORDS_POR_AREA.items():
                if area_lower in area_key.lower() or area_key.lower() in area_lower:
                    for kw in kw_list:
                        keywords.add(kw.lower())
                    break
            else:
                # Último recurso: usa o próprio nome da área como keyword
                keywords.add(area_lower)

    return list(keywords)


# ============================================================
# CONEXÃO COM BANCO DE DADOS
# ============================================================

def get_db_connection():
    """Cria conexão direta com PostgreSQL (mesmo padrão dos outros arquivos)"""
    try:
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT', 5432),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            client_encoding='utf8'
        )
        return conn
    except Exception as e:
        logger.error(f"Erro de conexão: {e}")
        return None


# ============================================================
# HELPER: BUSCAR SUBSCRIPTION ATIVA DO USUÁRIO
# ============================================================

def get_user_subscription(user_id, cursor):
    """
    Busca a subscription ativa mais recente do usuário.
    Retorna dict com selected_states e selected_areas (já parseados como listas),
    ou None se o usuário não tiver subscription ativa.
    """
    try:
        cursor.execute("""
            SELECT id, plan_name, selected_states, selected_areas, status, created_at
            FROM subscriptions
            WHERE user_id = %s AND status = 'active'
            ORDER BY created_at DESC
            LIMIT 1
        """, (user_id,))
        row = cursor.fetchone()

        if not row:
            return None

        states = []
        if row['selected_states']:
            try:
                raw = row['selected_states']
                states = json.loads(raw) if isinstance(raw, str) else raw
            except Exception:
                states = []

        areas = []
        if row['selected_areas']:
            try:
                raw = row['selected_areas']
                areas = json.loads(raw) if isinstance(raw, str) else raw
            except Exception:
                areas = []

        return {
            'subscription_id': row['id'],
            'plan_name': row['plan_name'],
            'selected_states': states,
            'selected_areas': areas,
            'status': row['status'],
            'created_at': row['created_at'].isoformat() if row['created_at'] else None
        }

    except Exception as e:
        logger.error(f"Erro ao buscar subscription do usuário {user_id}: {e}")
        return None


# ============================================================
# AUTENTICAÇÃO POR API KEY
# ============================================================

def get_user_by_api_key(api_key):
    """Busca o usuário dono da API Key informada."""
    if not api_key:
        return None

    conn = get_db_connection()
    if not conn:
        return None

    try:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cursor.execute("""
            SELECT id, username, email, full_name, phone,
                   company_name, state, user_type,
                   zaia_api_key, zaia_webhook_url
            FROM users
            WHERE zaia_api_key = %s AND is_active = true
        """, (api_key,))

        user = cursor.fetchone()

        if not user:
            cursor.close()
            conn.close()
            return None

        user_dict = dict(user)
        subscription = get_user_subscription(user_dict['id'], cursor)
        user_dict['subscription'] = subscription

        cursor.close()
        conn.close()
        return user_dict

    except Exception as e:
        logger.error(f"Erro ao buscar usuário por API Key: {e}")
        if conn:
            conn.close()
        return None


def require_api_key(f):
    """Decorador de autenticação via header X-API-Key."""
    from functools import wraps

    @wraps(f)
    def decorated(*args, **kwargs):
        api_key = request.headers.get('X-API-Key')

        if not api_key:
            return jsonify({
                'success': False,
                'error': 'Autenticação necessária. Informe o header X-API-Key.'
            }), 401

        user = get_user_by_api_key(api_key)

        if not user:
            return jsonify({
                'success': False,
                'error': 'API Key inválida ou usuário inativo.'
            }), 403

        kwargs['current_user'] = user
        return f(*args, **kwargs)

    return decorated


# ============================================================
# ENDPOINT 1: GERAR API KEY
# ============================================================

@zaia_bp.route('/zaia/gerar-api-key', methods=['POST'])
def gerar_api_key():
    """
    Gera uma nova API Key para o usuário.

    Body JSON:
    { "user_id": 123, "password": "senha_do_usuario" }
    """
    try:
        data = request.get_json()

        if not data or not data.get('user_id') or not data.get('password'):
            return jsonify({
                'success': False,
                'error': 'user_id e password são obrigatórios'
            }), 400

        conn = get_db_connection()
        if not conn:
            raise Exception("Erro de conexão com banco")

        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cursor.execute("""
            SELECT id, password_hash, is_active
            FROM users
            WHERE id = %s AND is_active = true
        """, (data['user_id'],))

        user = cursor.fetchone()

        if not user:
            return jsonify({'success': False, 'error': 'Usuário não encontrado'}), 404

        import hashlib
        import bcrypt as _bcrypt

        password_hash_db = user['password_hash']
        password_input = data['password']
        password_match = False

        if password_hash_db.startswith('$2b$') or password_hash_db.startswith('$2a$'):
            password_match = _bcrypt.checkpw(
                password_input.encode('utf-8'),
                password_hash_db.encode('utf-8')
            )
        else:
            sha256_hash = hashlib.sha256(password_input.encode()).hexdigest()
            password_match = (sha256_hash == password_hash_db)

        if not password_match:
            return jsonify({'success': False, 'error': 'Senha incorreta'}), 401

        new_api_key = 'zaia_sk_' + secrets.token_urlsafe(32)

        cursor.execute("""
            UPDATE users SET zaia_api_key = %s, updated_at = %s WHERE id = %s
        """, (new_api_key, datetime.now(), data['user_id']))

        conn.commit()
        cursor.close()
        conn.close()

        return jsonify({
            'success': True,
            'api_key': new_api_key,
            'mensagem': 'API Key gerada com sucesso! Guarde-a em local seguro, ela não será exibida novamente.'
        })

    except Exception as e:
        logger.error(f"Erro ao gerar API Key: {e}")
        return jsonify({'success': False, 'error': 'Erro interno do servidor'}), 500


# ============================================================
# ENDPOINT 2: PERFIL E PREFERÊNCIAS (VIA SUBSCRIPTION)
# ============================================================

@zaia_bp.route('/zaia/perfil', methods=['GET'])
@require_api_key
def get_perfil(current_user):
    """
    Retorna perfil completo do usuário.
    As preferências vêm diretamente da subscription ativa.
    """
    subscription = current_user.get('subscription')

    plano_info = None
    if subscription:
        areas = subscription.get('selected_areas', [])
        plano_info = {
            'nome': subscription.get('plan_name', ''),
            'status': subscription.get('status', ''),
            'estados_selecionados': subscription.get('selected_states', []),
            'areas_selecionadas': areas,
            'total_keywords_por_area': {
                area: len(KEYWORDS_POR_AREA.get(area, [area]))
                for area in areas
            },
            'assinado_em': subscription.get('created_at')
        }

    return jsonify({
        'success': True,
        'usuario': {
            'id': current_user['id'],
            'nome_completo': current_user.get('full_name', ''),
            'email': current_user.get('email', ''),
            'telefone': current_user.get('phone', ''),
            'nome_empresa': current_user.get('company_name'),
            'tipo_usuario': current_user.get('user_type', ''),
            'plano': plano_info,
            'tem_plano_ativo': subscription is not None,
            'webhook_configurado': bool(current_user.get('zaia_webhook_url'))
        }
    })


# ============================================================
# ENDPOINT 3: BUSCA DE LICITAÇÕES (KEYWORDS EXPANDIDAS)
# ============================================================

@zaia_bp.route('/zaia/licitacoes', methods=['GET'])
@require_api_key
def buscar_licitacoes(current_user):
    """
    Busca licitações com filtros avançados.
    Quando filtra por área, usa o dicionário expandido de palavras-chave
    para garantir cobertura máxima de licitações relacionadas.

    Header: X-API-Key: zaia_sk_xxxx

    Query params (todos opcionais):
    - q          : Palavra-chave livre
    - estados    : Siglas separadas por vírgula (ex: SP,MG)
    - areas      : Nomes de áreas separados por | (ex: Construção Civil|Saúde e Medicamentos)
    - valor_min  : Valor mínimo estimado
    - valor_max  : Valor máximo estimado
    - data_inicio: Data início publicação (YYYY-MM-DD)
    - data_fim   : Data fim publicação (YYYY-MM-DD)
    - apenas_novas: true/false — apenas últimas 24h
    - page       : Página (padrão: 1)
    - per_page   : Itens por página (padrão: 10, máximo: 50)
    """
    try:
        q = request.args.get('q', '').strip()
        estados_param = request.args.get('estados', '').strip()
        areas_param = request.args.get('areas', '').strip()
        valor_min = request.args.get('valor_min', type=float)
        valor_max = request.args.get('valor_max', type=float)
        data_inicio = request.args.get('data_inicio', '').strip()
        data_fim = request.args.get('data_fim', '').strip()
        apenas_novas = request.args.get('apenas_novas', 'false').lower() == 'true'
        page = max(1, request.args.get('page', 1, type=int))
        per_page = min(50, request.args.get('per_page', 10, type=int))

        subscription = current_user.get('subscription')

        # Resolver estados
        if estados_param:
            estados_lista = [e.strip().upper() for e in estados_param.split(',') if e.strip()]
            estados_fonte = 'parametro'
        elif subscription and subscription.get('selected_states'):
            estados_lista = [e.upper() for e in subscription['selected_states']]
            estados_fonte = 'subscription'
        else:
            estados_lista = []
            estados_fonte = 'nenhum'

        # Resolver áreas e expandir para keywords
        # Parâmetro via URL usa | como separador (evita conflito com vírgula em nomes)
        if areas_param:
            areas_lista = [a.strip() for a in areas_param.split('|') if a.strip()]
            areas_fonte = 'parametro'
        elif subscription and subscription.get('selected_areas'):
            areas_lista = subscription['selected_areas']
            areas_fonte = 'subscription'
        else:
            areas_lista = []
            areas_fonte = 'nenhum'

        # Expandir áreas para lista de keywords
        keywords_areas = get_keywords_para_areas(areas_lista) if areas_lista else []

        conn = get_db_connection()
        if not conn:
            raise Exception("Erro de conexão com banco")

        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        where_clauses = ["1=1"]
        params = []

        # Filtro por palavra-chave geral
        if q:
            where_clauses.append(
                "(title ILIKE %s OR objeto ILIKE %s OR organization_name ILIKE %s OR description ILIKE %s)"
            )
            params.extend([f'%{q}%', f'%{q}%', f'%{q}%', f'%{q}%'])

        # Filtro por estados
        if estados_lista:
            placeholders = ','.join(['%s'] * len(estados_lista))
            where_clauses.append(f"state_code IN ({placeholders})")
            params.extend(estados_lista)

        # Filtro por keywords de área (OR entre todas as keywords)
        # Uma licitação é incluída se qualquer keyword aparecer no título, objeto ou descrição
        if keywords_areas:
            kw_conditions = []
            for kw in keywords_areas:
                kw_conditions.append(
                    "(title ILIKE %s OR objeto ILIKE %s OR description ILIKE %s)"
                )
                params.extend([f'%{kw}%', f'%{kw}%', f'%{kw}%'])
            where_clauses.append(f"({' OR '.join(kw_conditions)})")

        # Filtro por valor
        if valor_min is not None:
            where_clauses.append("(COALESCE(valor_total_estimado, estimated_value) >= %s)")
            params.append(valor_min)

        if valor_max is not None:
            where_clauses.append("(COALESCE(valor_total_estimado, estimated_value) <= %s)")
            params.append(valor_max)

        # Filtro por data
        if data_inicio:
            where_clauses.append("publication_date >= %s")
            params.append(data_inicio)

        if data_fim:
            where_clauses.append("publication_date <= %s")
            params.append(data_fim)

        # Apenas novas (últimas 24h)
        if apenas_novas:
            where_clauses.append("created_at > NOW() - INTERVAL '24 hours'")

        where_sql = " AND ".join(where_clauses)

        # Contar total
        cursor.execute(f"SELECT COUNT(*) FROM tenders WHERE {where_sql}", params)
        total = cursor.fetchone()['count']
        total_pages = (total + per_page - 1) // per_page if total > 0 else 0
        offset = (page - 1) * per_page

        # Buscar licitações
        cursor.execute(f"""
            SELECT
                id, pncp_id, title, objeto, description,
                organization_name, organization_cnpj,
                municipality_name, state_code,
                publication_date, status, modality,
                estimated_value, valor_total_estimado,
                detail_url, source_url,
                items_count, downloads_count,
                created_at
            FROM tenders
            WHERE {where_sql}
            ORDER BY created_at DESC
            LIMIT %s OFFSET %s
        """, params + [per_page, offset])

        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        licitacoes = []
        for row in rows:
            valor = row['valor_total_estimado'] or row['estimated_value']
            licitacoes.append({
                'id': row['id'],
                'pncp_id': row['pncp_id'] or '',
                'titulo': row['title'] or '',
                'objeto': row['objeto'] or row['description'] or '',
                'orgao': row['organization_name'] or '',
                'cnpj_orgao': row['organization_cnpj'] or '',
                'municipio': row['municipality_name'] or '',
                'estado': row['state_code'] or '',
                'data_publicacao': row['publication_date'].isoformat() if row['publication_date'] else None,
                'status': row['status'] or 'Publicado',
                'modalidade': row['modality'] or '',
                'valor_estimado': float(valor) if valor else None,
                'valor_formatado': _format_currency(valor),
                'url_pncp': row['detail_url'] or row['source_url'] or '',
                'qtd_itens': row['items_count'] or 0,
                'qtd_arquivos': row['downloads_count'] or 0,
                'criado_em': row['created_at'].isoformat() if row['created_at'] else None
            })

        # Montar texto resumido para o agente Zaia usar diretamente na resposta
        if licitacoes:
            partes = [f"Encontrei {total} licitacao(oes) com esses criterios. Aqui estao as primeiras {len(licitacoes)}:"]
            for i, l in enumerate(licitacoes, 1):
                data_pub = l['data_publicacao'] or 'N/D'
                valor = l['valor_formatado'] or 'Nao informado'
                link = l['url_pncp'] or 'Nao disponivel'
                partes.append(
                    f"{i}. Titulo: {l['titulo']} | "
                    f"Orgao: {l['orgao']} | "
                    f"Local: {l['municipio']} - {l['estado']} | "
                    f"Valor: {valor} | "
                    f"Publicado em: {data_pub} | "
                    f"Link: {link}"
                )
            resumo_texto = " /// ".join(partes)
        else:
            resumo_texto = (
                "Nenhuma licitacao encontrada com os criterios informados. "
                "Tente ampliar a busca usando palavras-chave mais gerais ou removendo filtros de data."
            )

        return jsonify({
            'success': True,
            'resumo_texto': resumo_texto,
            'pagination': {
                'page': page,
                'per_page': per_page,
                'total': total,
                'total_pages': total_pages
            },
            'filtros_aplicados': {
                'busca': q or None,
                'estados': estados_lista or None,
                'estados_fonte': estados_fonte,
                'areas': areas_lista or None,
                'areas_fonte': areas_fonte,
                'total_keywords_expandidas': len(keywords_areas),
                'valor_min': valor_min,
                'valor_max': valor_max,
                'apenas_novas': apenas_novas
            },
            'licitacoes': licitacoes
        })

    except Exception as e:
        logger.error(f"Erro na busca Zaia: {e}")
        return jsonify({'success': False, 'error': 'Erro interno do servidor'}), 500


# ============================================================
# ENDPOINT 4: DETALHES DE UMA LICITAÇÃO
# ============================================================

@zaia_bp.route('/zaia/licitacoes/<int:licitacao_id>', methods=['GET'])
@require_api_key
def get_licitacao_detalhe(licitacao_id, current_user):
    """Retorna todos os detalhes de uma licitação específica."""
    try:
        conn = get_db_connection()
        if not conn:
            raise Exception("Erro de conexão com banco")

        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cursor.execute("""
            SELECT
                id, pncp_id, title, objeto, description, detailed_description,
                organization_name, organization_cnpj,
                municipality_name, municipality_ibge, state_code,
                publication_date, prazo, status, modality,
                estimated_value, valor_total_estimado,
                detail_url, source_url,
                items_json, downloaded_files_json,
                items_count, downloads_count,
                created_at, updated_at
            FROM tenders
            WHERE id = %s
        """, (licitacao_id,))

        row = cursor.fetchone()
        cursor.close()
        conn.close()

        if not row:
            return jsonify({'success': False, 'error': 'Licitação não encontrada'}), 404

        itens = []
        arquivos = []
        try:
            if row['items_json']:
                itens = json.loads(row['items_json'])
        except Exception:
            pass

        try:
            if row['downloaded_files_json']:
                arquivos = json.loads(row['downloaded_files_json'])
        except Exception:
            pass

        valor = row['valor_total_estimado'] or row['estimated_value']

        return jsonify({
            'success': True,
            'licitacao': {
                'id': row['id'],
                'pncp_id': row['pncp_id'] or '',
                'titulo': row['title'] or '',
                'objeto': row['objeto'] or row['description'] or '',
                'descricao_detalhada': row['detailed_description'] or '',
                'orgao': row['organization_name'] or '',
                'cnpj_orgao': row['organization_cnpj'] or '',
                'municipio': row['municipality_name'] or '',
                'ibge': row['municipality_ibge'] or '',
                'estado': row['state_code'] or '',
                'data_publicacao': row['publication_date'].isoformat() if row['publication_date'] else None,
                'prazo': row['prazo'] or '',
                'status': row['status'] or 'Publicado',
                'modalidade': row['modality'] or '',
                'valor_estimado': float(valor) if valor else None,
                'valor_formatado': _format_currency(valor),
                'url_pncp': row['detail_url'] or row['source_url'] or '',
                'itens': itens,
                'arquivos': arquivos,
                'qtd_itens': row['items_count'] or len(itens),
                'qtd_arquivos': row['downloads_count'] or len(arquivos),
                'criado_em': row['created_at'].isoformat() if row['created_at'] else None,
                'atualizado_em': row['updated_at'].isoformat() if row.get('updated_at') else None
            }
        })

    except Exception as e:
        logger.error(f"Erro ao buscar detalhe da licitação {licitacao_id}: {e}")
        return jsonify({'success': False, 'error': 'Erro interno do servidor'}), 500


# ============================================================
# ENDPOINT 5: CONFIGURAR WEBHOOK DA ZAIA
# ============================================================

@zaia_bp.route('/zaia/configurar-webhook', methods=['POST'])
@require_api_key
def configurar_webhook(current_user):
    """
    Salva a URL do webhook da Zaia para receber notificações automáticas.

    Body JSON:
    { "url_webhook": "https://api.zaia.ai/v1/incoming/seglicit/xxxxxxxx" }
    """
    try:
        data = request.get_json()

        if not data or not data.get('url_webhook'):
            return jsonify({'success': False, 'error': 'url_webhook é obrigatório'}), 400

        url_webhook = data['url_webhook'].strip()

        if not url_webhook.startswith('http'):
            return jsonify({'success': False, 'error': 'URL do webhook inválida'}), 400

        conn = get_db_connection()
        if not conn:
            raise Exception("Erro de conexão com banco")

        cursor = conn.cursor()
        cursor.execute("""
            UPDATE users SET zaia_webhook_url = %s, updated_at = %s WHERE id = %s
        """, (url_webhook, datetime.now(), current_user['id']))

        conn.commit()
        cursor.close()
        conn.close()

        return jsonify({
            'success': True,
            'mensagem': 'Webhook configurado com sucesso! Você receberá notificações automáticas de novas licitações.',
            'url_webhook': url_webhook
        })

    except Exception as e:
        logger.error(f"Erro ao configurar webhook: {e}")
        return jsonify({'success': False, 'error': 'Erro interno do servidor'}), 500


# ============================================================
# ENDPOINT 6: PING
# ============================================================

@zaia_bp.route('/zaia/ping', methods=['GET'])
def ping():
    """Endpoint público para verificar se a API Zaia está online."""
    return jsonify({
        'success': True,
        'mensagem': 'API Zaia está online!',
        'versao': '2.1.0',
        'endpoints': [
            'POST /api/zaia/gerar-api-key       - Gera API Key para o usuário',
            'GET  /api/zaia/perfil              - Perfil e preferências (requer X-API-Key)',
            'GET  /api/zaia/licitacoes          - Busca de licitações (requer X-API-Key)',
            'GET  /api/zaia/licitacoes/<id>     - Detalhe de licitação (requer X-API-Key)',
            'POST /api/zaia/configurar-webhook  - Configurar webhook (requer X-API-Key)',
            'GET  /api/zaia/ping               - Verificar status (público)'
        ],
        'areas_suportadas': list(KEYWORDS_POR_AREA.keys()),
        'timestamp': datetime.now().isoformat()
    })


# ============================================================
# FUNÇÃO INTERNA: DISPARAR WEBHOOK PARA USUÁRIOS RELEVANTES
# ============================================================

def disparar_webhooks_nova_licitacao(licitacao_dict):
    """
    Chamada pela automação após inserir nova licitação no banco.
    Usa o mesmo dicionário de keywords para verificar relevância.
    """
    try:
        conn = get_db_connection()
        if not conn:
            logger.error("Webhook: Erro de conexão com banco")
            return

        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cursor.execute("""
            SELECT
                u.id, u.full_name, u.email, u.zaia_webhook_url,
                s.plan_name, s.selected_states, s.selected_areas
            FROM users u
            INNER JOIN subscriptions s ON s.user_id = u.id AND s.status = 'active'
            WHERE u.zaia_webhook_url IS NOT NULL
              AND u.zaia_webhook_url != ''
              AND u.is_active = true
            ORDER BY s.created_at DESC
        """)

        usuarios = cursor.fetchall()
        cursor.close()
        conn.close()

        estado_licitacao = (licitacao_dict.get('estado') or licitacao_dict.get('state_code', '')).upper()
        texto_licitacao = (
            (licitacao_dict.get('titulo') or licitacao_dict.get('title', '')) + ' ' +
            (licitacao_dict.get('objeto') or licitacao_dict.get('description', ''))
        ).lower()

        notificados = set()

        for usuario in usuarios:
            if usuario['id'] in notificados:
                continue

            estados_usuario = []
            areas_usuario = []

            try:
                raw = usuario['selected_states']
                estados_usuario = json.loads(raw) if isinstance(raw, str) else (raw or [])
            except Exception:
                pass

            try:
                raw = usuario['selected_areas']
                areas_usuario = json.loads(raw) if isinstance(raw, str) else (raw or [])
            except Exception:
                pass

            # Verificar estado
            estado_ok = not estados_usuario or estado_licitacao in [e.upper() for e in estados_usuario]

            # Verificar área usando keywords expandidas
            area_ok = False
            if not areas_usuario:
                area_ok = True
            else:
                keywords = get_keywords_para_areas(areas_usuario)
                area_ok = any(kw in texto_licitacao for kw in keywords)

            if estado_ok and area_ok:
                _enviar_webhook(usuario['zaia_webhook_url'], usuario, licitacao_dict)
                notificados.add(usuario['id'])

    except Exception as e:
        logger.error(f"Erro ao disparar webhooks: {e}")


def _enviar_webhook(url_webhook, usuario, licitacao):
    """Envia a notificação POST para a URL do webhook da Zaia."""
    try:
        payload = {
            'evento': 'nova_licitacao_relevante',
            'timestamp': datetime.now().isoformat(),
            'usuario': {
                'id': usuario['id'],
                'nome': usuario.get('full_name', ''),
                'email': usuario.get('email', '')
            },
            'licitacao': {
                'id': licitacao.get('id'),
                'titulo': licitacao.get('titulo') or licitacao.get('title', ''),
                'objeto': licitacao.get('objeto') or licitacao.get('description', ''),
                'orgao': licitacao.get('orgao') or licitacao.get('organization_name', ''),
                'municipio': licitacao.get('municipio') or licitacao.get('municipality_name', ''),
                'estado': licitacao.get('estado') or licitacao.get('state_code', ''),
                'modalidade': licitacao.get('modalidade') or licitacao.get('modality', ''),
                'valor_estimado': licitacao.get('valor_estimado') or licitacao.get('estimated_value'),
                'valor_formatado': _format_currency(
                    licitacao.get('valor_estimado') or licitacao.get('estimated_value')
                ),
                'data_publicacao': str(licitacao.get('data_publicacao') or licitacao.get('publication_date', '')),
                'url_pncp': licitacao.get('url_pncp') or licitacao.get('detail_url', '')
            }
        }

        response = http_requests.post(
            url_webhook,
            json=payload,
            timeout=10,
            headers={'Content-Type': 'application/json'}
        )

        if response.status_code in (200, 201, 202):
            logger.info(f"Webhook enviado com sucesso para usuário ID {usuario['id']}")
        else:
            logger.warning(f"Webhook retornou status {response.status_code} para usuário ID {usuario['id']}")

    except Exception as e:
        logger.error(f"Erro ao enviar webhook para {url_webhook}: {e}")


# ============================================================
# UTILITÁRIOS
# ============================================================

def _format_currency(value):
    """Formata valor para moeda brasileira."""
    if not value:
        return 'Valor não informado'
    try:
        val = float(value)
        formatted = f"R$ {val:,.2f}"
        formatted = formatted.replace(',', 'TEMP').replace('.', ',').replace('TEMP', '.')
        return formatted
    except Exception:
        return 'Valor não informado'