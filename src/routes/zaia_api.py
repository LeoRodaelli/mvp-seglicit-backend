# -*- coding: utf-8 -*-
"""
API de Integração com o Agente de IA Zaia
==========================================
Endpoints exclusivos para o agente Zaia acessar dados da plataforma Seglicit.

Autenticação: Header X-API-Key com chave única por usuário.

Endpoints disponíveis:
  GET  /api/zaia/perfil              - Perfil e preferências do usuário
  GET  /api/zaia/licitacoes          - Busca de licitações com filtros
  GET  /api/zaia/licitacoes/<id>     - Detalhes de uma licitação específica
  POST /api/zaia/configurar-webhook  - Salva URL do webhook da Zaia
  POST /api/zaia/notificar           - (Interno) Dispara webhook para usuário

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
# AUTENTICAÇÃO POR API KEY
# ============================================================

def get_user_by_api_key(api_key):
    """
    Busca o usuário dono da API Key informada.
    Retorna o dict do usuário ou None se a chave for inválida.
    """
    if not api_key:
        return None

    conn = get_db_connection()
    if not conn:
        return None

    try:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute("""
            SELECT u.id, u.username, u.email, u.full_name, u.phone,
                   u.company_name, u.state, u.user_type,
                   u.zaia_api_key, u.zaia_webhook_url,
                   u.zaia_categorias, u.zaia_estados
            FROM users u
            WHERE u.zaia_api_key = %s AND u.is_active = true
        """, (api_key,))
        user = cursor.fetchone()
        cursor.close()
        conn.close()
        return dict(user) if user else None
    except Exception as e:
        logger.error(f"Erro ao buscar usuário por API Key: {e}")
        if conn:
            conn.close()
        return None


def require_api_key(f):
    """
    Decorador de autenticação.
    Verifica o header X-API-Key e injeta o usuário na função.
    Uso: @require_api_key acima da função de rota.
    """
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

        # Injeta o usuário como kwarg para a função de rota
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
    O usuário precisa estar autenticado (envia user_id + password_hash via body).

    Body JSON:
    {
        "user_id": 123,
        "password": "senha_do_usuario"
    }

    Resposta:
    {
        "success": true,
        "api_key": "zaia_sk_xxxxxxxxxxxxxxxxxxxx",
        "mensagem": "Guarde esta chave em local seguro."
    }
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

        # Buscar usuário e verificar senha
        cursor.execute("""
            SELECT id, password_hash, is_active
            FROM users
            WHERE id = %s AND is_active = true
        """, (data['user_id'],))

        user = cursor.fetchone()

        if not user:
            return jsonify({'success': False, 'error': 'Usuário não encontrado'}), 404

        # Verificar senha (suporta bcrypt e SHA256 como no user.py existente)
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

        # Gerar nova API Key
        new_api_key = 'zaia_sk_' + secrets.token_urlsafe(32)

        # Salvar no banco
        cursor.execute("""
            UPDATE users SET zaia_api_key = %s, updated_at = %s WHERE id = %s
        """, (new_api_key, datetime.now(), data['user_id']))

        conn.commit()
        cursor.close()
        conn.close()

        logger.info(f"Nova API Key gerada para usuário ID {data['user_id']}")

        return jsonify({
            'success': True,
            'api_key': new_api_key,
            'mensagem': 'API Key gerada com sucesso! Guarde-a em local seguro, ela não será exibida novamente.'
        })

    except Exception as e:
        logger.error(f"Erro ao gerar API Key: {e}")
        return jsonify({'success': False, 'error': 'Erro interno do servidor'}), 500


# ============================================================
# ENDPOINT 2: PERFIL E PREFERÊNCIAS DO USUÁRIO
# ============================================================

@zaia_bp.route('/zaia/perfil', methods=['GET'])
@require_api_key
def get_perfil(current_user):
    """
    Retorna o perfil completo do usuário autenticado, incluindo
    suas preferências de busca (categorias e estados).

    Header: X-API-Key: zaia_sk_xxxx

    Resposta:
    {
        "success": true,
        "usuario": {
            "id": 123,
            "nome_completo": "João da Silva",
            "email": "joao@empresa.com",
            "telefone": "(11) 99999-8888",
            "nome_empresa": "Construtora Silva",
            "preferencias": {
                "categorias": ["Construção Civil", "Engenharia"],
                "estados": ["SP", "MG"]
            }
        }
    }
    """
    try:
        # Parsear preferências salvas como JSON no banco
        categorias = []
        estados = []

        if current_user.get('zaia_categorias'):
            try:
                categorias = json.loads(current_user['zaia_categorias'])
            except Exception:
                categorias = []

        if current_user.get('zaia_estados'):
            try:
                estados = json.loads(current_user['zaia_estados'])
            except Exception:
                # Se não tem preferências salvas, usa o estado do cadastro
                if current_user.get('state'):
                    estados = [current_user['state']]

        return jsonify({
            'success': True,
            'usuario': {
                'id': current_user['id'],
                'nome_completo': current_user.get('full_name', ''),
                'email': current_user.get('email', ''),
                'telefone': current_user.get('phone', ''),
                'nome_empresa': current_user.get('company_name', ''),
                'tipo_usuario': current_user.get('user_type', ''),
                'preferencias': {
                    'categorias': categorias,
                    'estados': estados
                },
                'webhook_configurado': bool(current_user.get('zaia_webhook_url'))
            }
        })

    except Exception as e:
        logger.error(f"Erro ao buscar perfil Zaia: {e}")
        return jsonify({'success': False, 'error': 'Erro interno do servidor'}), 500


# ============================================================
# ENDPOINT 3: BUSCA DE LICITAÇÕES
# ============================================================

@zaia_bp.route('/zaia/licitacoes', methods=['GET'])
@require_api_key
def buscar_licitacoes(current_user):
    """
    Busca licitações com filtros avançados.
    Se nenhum filtro for passado, usa as preferências do usuário automaticamente.

    Header: X-API-Key: zaia_sk_xxxx

    Query params:
    - q          : Palavra-chave (busca em título, objeto, órgão)
    - estados    : Siglas separadas por vírgula (ex: SP,MG,RJ)
    - categorias : Palavras-chave de categoria separadas por vírgula
    - valor_min  : Valor mínimo estimado (ex: 10000)
    - valor_max  : Valor máximo estimado (ex: 500000)
    - data_inicio: Data de publicação início (YYYY-MM-DD)
    - data_fim   : Data de publicação fim (YYYY-MM-DD)
    - apenas_novas: true/false - apenas licitações das últimas 24h
    - page       : Página (padrão: 1)
    - per_page   : Itens por página (padrão: 10, máximo: 50)

    Resposta:
    {
        "success": true,
        "pagination": { "page": 1, "total": 48, "total_pages": 5 },
        "licitacoes": [ { ... } ]
    }
    """
    try:
        # Parâmetros da requisição
        q = request.args.get('q', '').strip()
        estados_param = request.args.get('estados', '').strip()
        categorias_param = request.args.get('categorias', '').strip()
        valor_min = request.args.get('valor_min', type=float)
        valor_max = request.args.get('valor_max', type=float)
        data_inicio = request.args.get('data_inicio', '').strip()
        data_fim = request.args.get('data_fim', '').strip()
        apenas_novas = request.args.get('apenas_novas', 'false').lower() == 'true'
        page = max(1, request.args.get('page', 1, type=int))
        per_page = min(50, request.args.get('per_page', 10, type=int))

        # Se não passou estados, usa as preferências do usuário
        if not estados_param and current_user.get('zaia_estados'):
            try:
                estados_lista = json.loads(current_user['zaia_estados'])
            except Exception:
                estados_lista = []
        elif estados_param:
            estados_lista = [e.strip().upper() for e in estados_param.split(',') if e.strip()]
        else:
            estados_lista = []

        # Se não passou categorias, usa as preferências do usuário
        if not categorias_param and current_user.get('zaia_categorias'):
            try:
                categorias_lista = json.loads(current_user['zaia_categorias'])
            except Exception:
                categorias_lista = []
        elif categorias_param:
            categorias_lista = [c.strip() for c in categorias_param.split(',') if c.strip()]
        else:
            categorias_lista = []

        conn = get_db_connection()
        if not conn:
            raise Exception("Erro de conexão com banco")

        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # Construir query base
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

        # Filtro por categorias (busca nas palavras-chave no título/objeto)
        if categorias_lista:
            cat_conditions = []
            for cat in categorias_lista:
                cat_conditions.append("(title ILIKE %s OR objeto ILIKE %s)")
                params.extend([f'%{cat}%', f'%{cat}%'])
            where_clauses.append(f"({' OR '.join(cat_conditions)})")

        # Filtro por valor
        if valor_min is not None:
            where_clauses.append(
                "(COALESCE(valor_total_estimado, estimated_value) >= %s)"
            )
            params.append(valor_min)

        if valor_max is not None:
            where_clauses.append(
                "(COALESCE(valor_total_estimado, estimated_value) <= %s)"
            )
            params.append(valor_max)

        # Filtro por data de publicação
        if data_inicio:
            where_clauses.append("publication_date >= %s")
            params.append(data_inicio)

        if data_fim:
            where_clauses.append("publication_date <= %s")
            params.append(data_fim)

        # Filtro apenas novas (últimas 24h)
        if apenas_novas:
            where_clauses.append("created_at > NOW() - INTERVAL '24 hours'")

        where_sql = " AND ".join(where_clauses)

        # Contar total para paginação
        cursor.execute(f"SELECT COUNT(*) FROM tenders WHERE {where_sql}", params)
        total = cursor.fetchone()['count']
        total_pages = (total + per_page - 1) // per_page
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

        # Formatar resultado
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

        return jsonify({
            'success': True,
            'pagination': {
                'page': page,
                'per_page': per_page,
                'total': total,
                'total_pages': total_pages
            },
            'filtros_aplicados': {
                'busca': q or None,
                'estados': estados_lista or None,
                'categorias': categorias_lista or None,
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
    """
    Retorna todos os detalhes de uma licitação específica, incluindo
    itens e arquivos disponíveis.

    Header: X-API-Key: zaia_sk_xxxx

    Resposta:
    {
        "success": true,
        "licitacao": { ... todos os campos ... }
    }
    """
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

        # Parsear itens e arquivos
        itens = []
        arquivos = []
        try:
            if row['items_json']:
                itens = json.loads(row['items_json'])
        except Exception:
            itens = []

        try:
            if row['downloaded_files_json']:
                arquivos = json.loads(row['downloaded_files_json'])
        except Exception:
            arquivos = []

        valor = row['valor_total_estimado'] or row['estimated_value']

        licitacao = {
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

        return jsonify({
            'success': True,
            'licitacao': licitacao
        })

    except Exception as e:
        logger.error(f"Erro ao buscar detalhe da licitação {licitacao_id}: {e}")
        return jsonify({'success': False, 'error': 'Erro interno do servidor'}), 500


# ============================================================
# ENDPOINT 5: CONFIGURAR PREFERÊNCIAS DO USUÁRIO
# ============================================================

@zaia_bp.route('/zaia/preferencias', methods=['POST'])
@require_api_key
def salvar_preferencias(current_user):
    """
    Salva ou atualiza as preferências de busca do usuário (categorias e estados).
    A Zaia pode chamar este endpoint quando o usuário mudar suas preferências no chat.

    Header: X-API-Key: zaia_sk_xxxx

    Body JSON:
    {
        "categorias": ["Construção Civil", "Obras de Pavimentação"],
        "estados": ["SP", "MG", "RJ"]
    }
    """
    try:
        data = request.get_json()

        if not data:
            return jsonify({'success': False, 'error': 'Body JSON obrigatório'}), 400

        categorias = data.get('categorias', [])
        estados = data.get('estados', [])

        if not isinstance(categorias, list) or not isinstance(estados, list):
            return jsonify({'success': False, 'error': 'categorias e estados devem ser listas'}), 400

        conn = get_db_connection()
        if not conn:
            raise Exception("Erro de conexão com banco")

        cursor = conn.cursor()
        cursor.execute("""
            UPDATE users
            SET zaia_categorias = %s, zaia_estados = %s, updated_at = %s
            WHERE id = %s
        """, (
            json.dumps(categorias, ensure_ascii=False),
            json.dumps(estados, ensure_ascii=False),
            datetime.now(),
            current_user['id']
        ))

        conn.commit()
        cursor.close()
        conn.close()

        logger.info(f"Preferências atualizadas para usuário ID {current_user['id']}: {len(categorias)} categorias, {len(estados)} estados")

        return jsonify({
            'success': True,
            'mensagem': 'Preferências salvas com sucesso!',
            'preferencias': {
                'categorias': categorias,
                'estados': estados
            }
        })

    except Exception as e:
        logger.error(f"Erro ao salvar preferências: {e}")
        return jsonify({'success': False, 'error': 'Erro interno do servidor'}), 500


# ============================================================
# ENDPOINT 6: CONFIGURAR WEBHOOK DA ZAIA
# ============================================================

@zaia_bp.route('/zaia/configurar-webhook', methods=['POST'])
@require_api_key
def configurar_webhook(current_user):
    """
    Salva a URL do webhook da Zaia para receber notificações automáticas
    quando novas licitações relevantes forem encontradas.

    Header: X-API-Key: zaia_sk_xxxx

    Body JSON:
    {
        "url_webhook": "https://api.zaia.ai/v1/incoming/seglicit/xxxxxxxx"
    }
    """
    try:
        data = request.get_json()

        if not data or not data.get('url_webhook'):
            return jsonify({'success': False, 'error': 'url_webhook é obrigatório'}), 400

        url_webhook = data['url_webhook'].strip()

        # Validação básica da URL
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

        logger.info(f"Webhook configurado para usuário ID {current_user['id']}: {url_webhook}")

        return jsonify({
            'success': True,
            'mensagem': 'Webhook configurado com sucesso! Você receberá notificações automáticas de novas licitações.',
            'url_webhook': url_webhook
        })

    except Exception as e:
        logger.error(f"Erro ao configurar webhook: {e}")
        return jsonify({'success': False, 'error': 'Erro interno do servidor'}), 500


# ============================================================
# FUNÇÃO INTERNA: DISPARAR WEBHOOK PARA USUÁRIOS RELEVANTES
# ============================================================

def disparar_webhooks_nova_licitacao(licitacao_dict):
    """
    Função chamada pela automação após inserir uma nova licitação no banco.
    Busca todos os usuários com webhook configurado e preferências compatíveis,
    e envia a notificação para cada um.

    Uso no script de automação:
        from src.routes.zaia_api import disparar_webhooks_nova_licitacao
        disparar_webhooks_nova_licitacao(nova_licitacao)

    Parâmetro licitacao_dict deve conter:
        - id, titulo, objeto, orgao, municipio, estado, modalidade,
          valor_estimado, data_publicacao, url_pncp
    """
    try:
        conn = get_db_connection()
        if not conn:
            logger.error("Webhook: Erro de conexão com banco")
            return

        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # Buscar todos os usuários com webhook configurado
        cursor.execute("""
            SELECT id, full_name, email, zaia_webhook_url, zaia_categorias, zaia_estados
            FROM users
            WHERE zaia_webhook_url IS NOT NULL
              AND zaia_webhook_url != ''
              AND is_active = true
        """)

        usuarios = cursor.fetchall()
        cursor.close()
        conn.close()

        estado_licitacao = licitacao_dict.get('estado', '').upper()
        titulo_licitacao = (licitacao_dict.get('titulo', '') + ' ' + licitacao_dict.get('objeto', '')).lower()

        for usuario in usuarios:
            # Verificar se a licitação é relevante para este usuário
            relevante = False

            # Parsear preferências
            estados_usuario = []
            categorias_usuario = []

            try:
                if usuario['zaia_estados']:
                    estados_usuario = json.loads(usuario['zaia_estados'])
            except Exception:
                pass

            try:
                if usuario['zaia_categorias']:
                    categorias_usuario = json.loads(usuario['zaia_categorias'])
            except Exception:
                pass

            # Verificar estado
            estado_ok = not estados_usuario or estado_licitacao in [e.upper() for e in estados_usuario]

            # Verificar categoria (palavra-chave no título/objeto)
            categoria_ok = not categorias_usuario or any(
                cat.lower() in titulo_licitacao for cat in categorias_usuario
            )

            relevante = estado_ok and categoria_ok

            if relevante:
                _enviar_webhook(usuario['zaia_webhook_url'], usuario, licitacao_dict)

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
                'objeto': licitacao.get('objeto', ''),
                'orgao': licitacao.get('orgao') or licitacao.get('organization_name', ''),
                'municipio': licitacao.get('municipio') or licitacao.get('municipality_name', ''),
                'estado': licitacao.get('estado') or licitacao.get('state_code', ''),
                'modalidade': licitacao.get('modalidade') or licitacao.get('modality', ''),
                'valor_estimado': licitacao.get('valor_estimado') or licitacao.get('estimated_value'),
                'valor_formatado': _format_currency(
                    licitacao.get('valor_estimado') or licitacao.get('estimated_value')
                ),
                'data_publicacao': licitacao.get('data_publicacao') or licitacao.get('publication_date', ''),
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
            logger.info(f"Webhook enviado com sucesso para usuário ID {usuario['id']}: {url_webhook}")
        else:
            logger.warning(f"Webhook retornou status {response.status_code} para usuário ID {usuario['id']}")

    except Exception as e:
        logger.error(f"Erro ao enviar webhook para {url_webhook}: {e}")


# ============================================================
# ENDPOINT 7: TESTE DA API
# ============================================================

@zaia_bp.route('/zaia/ping', methods=['GET'])
def ping():
    """
    Endpoint público para verificar se a API Zaia está online.
    Não requer autenticação.
    """
    return jsonify({
        'success': True,
        'mensagem': 'API Zaia está online!',
        'versao': '1.0.0',
        'endpoints': [
            'POST /api/zaia/gerar-api-key       - Gera API Key para o usuário',
            'GET  /api/zaia/perfil              - Perfil e preferências (requer X-API-Key)',
            'GET  /api/zaia/licitacoes          - Busca de licitações (requer X-API-Key)',
            'GET  /api/zaia/licitacoes/<id>     - Detalhe de licitação (requer X-API-Key)',
            'POST /api/zaia/preferencias        - Salvar preferências (requer X-API-Key)',
            'POST /api/zaia/configurar-webhook  - Configurar webhook (requer X-API-Key)',
            'GET  /api/zaia/ping               - Verificar status (público)'
        ],
        'timestamp': datetime.now().isoformat()
    })


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
