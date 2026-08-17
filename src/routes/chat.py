# -*- coding: utf-8 -*-
"""
Chat interno com IA (Claude) para busca de licitações.

Alternativa ao agente da Zaia — não depende dela decidir chamar uma Ação
HTTP. O próprio backend interpreta a mensagem do usuário via tool use da
Claude API e busca as licitações diretamente, já filtradas pelo plano.
"""
import json
import logging
import os

from flask import Blueprint, jsonify, request

logger = logging.getLogger(__name__)

chat_bp = Blueprint('chat_bp', __name__)

MODEL_ID = 'claude-opus-5'
MAX_HISTORICO_TURNOS = 12  # limita custo/latência em conversas longas

BUSCAR_LICITACOES_TOOL = {
    "name": "buscar_licitacoes",
    "description": (
        "Busca licitações públicas disponíveis para o usuário, já filtradas pelo plano dele "
        "(estados e áreas de atuação contratados). Use sempre que o usuário pedir para ver, "
        "listar, procurar ou filtrar licitações — nunca invente resultados."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "q": {
                "type": "string",
                "description": "Palavra-chave livre para buscar no título/objeto/órgão (opcional).",
            },
            "estados": {
                "type": "string",
                "description": (
                    "Siglas de estado separadas por vírgula, ex: 'SP,RJ'. "
                    "Deixe vazio para usar todos os estados do plano do usuário."
                ),
            },
            "areas": {
                "type": "string",
                "description": (
                    "Nomes de área separados por '|', ex: 'Tecnologia e TI|Saúde e Medicamentos'. "
                    "Deixe vazio para usar todas as áreas do plano do usuário."
                ),
            },
            "data_inicio": {
                "type": "string",
                "description": "Data mínima de publicação, formato YYYY-MM-DD (opcional).",
            },
            "data_fim": {
                "type": "string",
                "description": "Data máxima de publicação, formato YYYY-MM-DD (opcional).",
            },
        },
        "required": [],
    },
}

SYSTEM_PROMPT = (
    "Você é o Assistente Seglicit, ajuda o usuário a encontrar licitações públicas na "
    "plataforma Seglicit. Sempre que o usuário pedir para ver, buscar ou listar licitações, "
    "use a ferramenta buscar_licitacoes — nunca invente licitações ou dados. Se a ferramenta "
    "retornar um erro (ex: sem plano ativo, estado fora do plano contratado), explique o "
    "problema ao usuário de forma clara e direta, sugerindo o que fazer a seguir. Ao "
    "apresentar resultados, liste para cada licitação: título, órgão, município/estado, "
    "valor estimado, data de publicação e o link. Seja direto e objetivo. Responda sempre "
    "em português do Brasil."
)


def _authenticate_chat_user():
    """Autentica o usuário do chat via X-API-Key (mesma zaia_api_key da plataforma)."""
    from src.routes.zaia_api import get_user_by_api_key

    api_key = (request.headers.get('X-API-Key') or '').strip()
    if not api_key:
        return None, 'Não foi possível autenticar sua sessão. Faça login novamente.'

    user = get_user_by_api_key(api_key)
    if not user:
        return None, 'Sua chave de acesso é inválida ou expirou. Faça logout e login novamente.'

    return user, None


def _build_messages_from_historico(historico, mensagem):
    """Reconstrói a lista de mensagens (só texto) a partir do histórico enviado pelo cliente."""
    messages = []
    for turno in (historico or [])[-MAX_HISTORICO_TURNOS:]:
        role = turno.get('role')
        content = turno.get('content')
        if role in ('user', 'assistant') and isinstance(content, str) and content.strip():
            messages.append({'role': role, 'content': content})
    messages.append({'role': 'user', 'content': mensagem})
    return messages


@chat_bp.route('/chat/mensagem', methods=['POST'])
def chat_mensagem():
    """
    Recebe uma mensagem em texto livre, usa Claude (tool use) para decidir
    se/como buscar licitações, executa a busca já filtrada pelo plano do
    usuário, e devolve a resposta final em texto.
    """
    try:
        import anthropic
    except ImportError:
        logger.error('Pacote anthropic não instalado no backend.')
        return jsonify({'success': False, 'resposta': 'Assistente indisponível no momento.'}), 500

    api_key = os.getenv('ANTHROPIC_API_KEY')
    if not api_key:
        logger.error('ANTHROPIC_API_KEY não configurada.')
        return jsonify({'success': False, 'resposta': 'Assistente indisponível no momento.'}), 500

    current_user, auth_error = _authenticate_chat_user()
    if auth_error:
        return jsonify({'success': False, 'resposta': auth_error}), 200

    data = request.get_json(silent=True) or {}
    mensagem = (data.get('mensagem') or '').strip()
    historico = data.get('historico') or []

    if not mensagem:
        return jsonify({'success': False, 'resposta': 'Mensagem vazia.'}), 200

    messages = _build_messages_from_historico(historico, mensagem)

    from src.services.tender_search_service import search_tenders_for_user

    client = anthropic.Anthropic(api_key=api_key)

    try:
        response = client.messages.create(
            model=MODEL_ID,
            max_tokens=2048,
            system=SYSTEM_PROMPT,
            tools=[BUSCAR_LICITACOES_TOOL],
            messages=messages,
        )

        max_iteracoes = 3
        iteracoes = 0
        while response.stop_reason == 'tool_use' and iteracoes < max_iteracoes:
            iteracoes += 1
            messages.append({'role': 'assistant', 'content': response.content})

            tool_results = []
            for block in response.content:
                if block.type != 'tool_use' or block.name != 'buscar_licitacoes':
                    continue
                args = block.input or {}
                resultado = search_tenders_for_user(
                    current_user,
                    q=args.get('q', ''),
                    estados_param=args.get('estados', ''),
                    areas_param=args.get('areas', ''),
                    data_inicio=args.get('data_inicio', ''),
                    data_fim=args.get('data_fim', ''),
                )
                tool_results.append({
                    'type': 'tool_result',
                    'tool_use_id': block.id,
                    'content': json.dumps(resultado, ensure_ascii=False),
                })

            if not tool_results:
                break

            messages.append({'role': 'user', 'content': tool_results})

            response = client.messages.create(
                model=MODEL_ID,
                max_tokens=2048,
                system=SYSTEM_PROMPT,
                tools=[BUSCAR_LICITACOES_TOOL],
                messages=messages,
            )

        texto_final = next((b.text for b in response.content if b.type == 'text'), '')
        if not texto_final:
            texto_final = 'Não consegui gerar uma resposta agora. Tente novamente.'

        return jsonify({'success': True, 'resposta': texto_final})

    except Exception as e:
        logger.error('Erro no chat com Claude: %s', e)
        return jsonify({
            'success': False,
            'resposta': 'Erro ao processar sua mensagem. Tente novamente em instantes.',
        }), 200
