# -*- coding: utf-8 -*-
"""
Envio de alertas de licitação nova via WhatsApp, usando a API oficial da Meta
(WhatsApp Cloud API). Como é uma mensagem iniciada pela empresa (não resposta
a um atendimento), só pode ser enviada como um "message template" já
aprovado pela Meta no WhatsApp Manager — texto livre não é permitido aqui.

Configuração necessária (.env):
  WHATSAPP_ACCESS_TOKEN     - token permanente do System User (Business Manager)
  WHATSAPP_PHONE_NUMBER_ID  - ID do número de telefone do WhatsApp Business
  WHATSAPP_TEMPLATE_NAME    - nome do template aprovado (ex: licitacoes_novas_alerta)
  WHATSAPP_TEMPLATE_LANG    - idioma do template (ex: pt_BR)

Template sugerido pra submeter no WhatsApp Manager (categoria "Utility"):
  "Olá {{1}}! 🔔 Encontramos {{2}} nova(s) licitação(ões) no seu plano
   Seglicit. Acesse a plataforma para ver os detalhes: {{3}}"
  (variáveis: 1=nome do usuário, 2=quantidade, 3=link de login)
"""
import logging
import os
import re

import requests

logger = logging.getLogger(__name__)

GRAPH_API_VERSION = 'v21.0'


def normalize_phone_e164(phone):
    """
    Normaliza um telefone salvo (só dígitos, DDD+número, sem código de país)
    pro formato exigido pela API (código de país + DDD + número, só dígitos).
    Assume Brasil (55) quando o número não já vem com o código do país.
    """
    if not phone:
        return None
    digits = re.sub(r'\D', '', str(phone))
    if not digits:
        return None
    if digits.startswith('55') and len(digits) in (12, 13):
        return digits
    if len(digits) in (10, 11):
        return f'55{digits}'
    return digits


def _send_template_message(to_phone_e164, template_name, template_lang, body_params):
    access_token = os.getenv('WHATSAPP_ACCESS_TOKEN')
    phone_number_id = os.getenv('WHATSAPP_PHONE_NUMBER_ID')

    if not access_token or not phone_number_id:
        logger.warning("⚠️ WHATSAPP_ACCESS_TOKEN/WHATSAPP_PHONE_NUMBER_ID não configurados — WhatsApp não enviado")
        return False

    url = f'https://graph.facebook.com/{GRAPH_API_VERSION}/{phone_number_id}/messages'
    payload = {
        'messaging_product': 'whatsapp',
        'to': to_phone_e164,
        'type': 'template',
        'template': {
            'name': template_name,
            'language': {'code': template_lang},
            'components': [
                {
                    'type': 'body',
                    'parameters': [{'type': 'text', 'text': str(p)} for p in body_params],
                }
            ],
        },
    }

    try:
        resp = requests.post(
            url,
            headers={
                'Authorization': f'Bearer {access_token}',
                'Content-Type': 'application/json',
            },
            json=payload,
            timeout=15,
        )
        if resp.status_code in (200, 201):
            return True
        logger.warning(f"WhatsApp: falha ao enviar pra {to_phone_e164} ({resp.status_code}): {resp.text[:300]}")
        return False
    except Exception as exc:
        logger.error(f"WhatsApp: erro ao enviar pra {to_phone_e164}: {exc}")
        return False


def send_tenders_whatsapp_alert(phone, user_name, tenders_count):
    """Envia o template de alerta de licitações novas para o telefone informado."""
    to_phone = normalize_phone_e164(phone)
    if not to_phone:
        logger.warning("WhatsApp: telefone inválido/ausente, envio ignorado")
        return False

    template_name = os.getenv('WHATSAPP_TEMPLATE_NAME', 'licitacoes_novas_alerta')
    template_lang = os.getenv('WHATSAPP_TEMPLATE_LANG', 'pt_BR')
    frontend_url = os.getenv('FRONTEND_URL', 'https://seglicit.com.br')
    login_link = f'{frontend_url}/login'

    primeiro_nome = (user_name or 'Cliente').strip().split(' ')[0]

    logger.info(f"📲 Enviando alerta WhatsApp para {to_phone}: {tenders_count} licitação(ões)")
    return _send_template_message(
        to_phone,
        template_name,
        template_lang,
        body_params=[primeiro_nome, tenders_count, login_link],
    )
