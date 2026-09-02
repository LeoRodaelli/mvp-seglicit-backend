# -*- coding: utf-8 -*-
"""
API para integração com Mercado Pago
Gerencia criação de preferências de pagamento, webhooks e consultas de status
"""

from flask import Blueprint, request, jsonify
import mercadopago
import os
import json
import logging
import traceback
from datetime import datetime
from dotenv import load_dotenv
import psycopg2
import psycopg2.extras
import bcrypt
import hmac
import hashlib

from src.services.subscription_billing import (
    activate_subscription_from_reference,
    fetch_authorized_payment,
    process_authorized_payment_notification,
    update_subscription_by_preapproval_status,
)

PAYMENT_STATUS_MESSAGES = {
    'cc_rejected_bad_filled_card_number': 'Número do cartão inválido.',
    'cc_rejected_bad_filled_date': 'Data de validade incorreta.',
    'cc_rejected_bad_filled_security_code': 'Código de segurança incorreto.',
    'cc_rejected_insufficient_amount': 'Saldo ou limite insuficiente no cartão.',
    'cc_rejected_high_risk': 'Pagamento recusado por segurança. Use o mesmo email do checkout na conta Mercado Pago.',
    'cc_rejected_call_for_authorize': 'Ligue para o banco para autorizar a compra.',
    'cc_rejected_card_disabled': 'Cartão desabilitado para compras online.',
    'cc_rejected_duplicated_payment': 'Pagamento duplicado detectado.',
    'cc_rejected_other_reason': 'Cartão recusado pelo banco emissor.',
}


def _payment_status_detail_message(status_detail):
    if not status_detail:
        return None
    return PAYMENT_STATUS_MESSAGES.get(
        status_detail,
        'Pagamento recusado. Verifique se o email no Mercado Pago é o mesmo do checkout.',
    )

load_dotenv()

# Configure logging
logger = logging.getLogger(__name__)

# Criar Blueprint
mercadopago_bp = Blueprint('mercadopago', __name__)

# Inicializar SDK do Mercado Pago
sdk = mercadopago.SDK(os.getenv('MERCADOPAGO_ACCESS_TOKEN'))


def _verify_webhook_signature(request):
    """
    Valida assinatura do webhook do MercadoPago.
    Docs: https://www.mercadopago.com.br/developers/pt/docs/your-integrations/notifications/webhooks
    """
    secret = os.getenv('MERCADOPAGO_WEBHOOK_SECRET')
    if not secret:
        logger.warning("⚠️ MERCADOPAGO_WEBHOOK_SECRET não configurado — pulando validação")
        return True

    x_signature = request.headers.get('x-signature', '')
    x_request_id = request.headers.get('x-request-id', '')

    if not x_signature or not x_request_id:
        logger.warning("⚠️ Headers x-signature ou x-request-id ausentes — pulando validação")
        return True

    # Extrair ts e v1 do header x-signature (formato: ts=<timestamp>,v1=<hash>)
    ts = None
    v1 = None
    for part in x_signature.split(','):
        key, _, value = part.strip().partition('=')
        if key == 'ts':
            ts = value
        elif key == 'v1':
            v1 = value

    if not ts or not v1:
        logger.error("❌ Formato inválido do header x-signature")
        return False

    # data.id vem sempre como query param na URL enviada pelo MercadoPago
    data_id = request.args.get('data.id', '')

    manifest = f"id:{data_id};request-id:{x_request_id};ts:{ts};"

    logger.info(f"🔍 WEBHOOK SIGNATURE DEBUG:")
    logger.info(f"   x-signature header : {x_signature}")
    logger.info(f"   x-request-id header: {x_request_id}")
    logger.info(f"   data.id query param: {data_id}")
    logger.info(f"   ts extraído        : {ts}")
    logger.info(f"   v1 recebido        : {v1}")
    logger.info(f"   manifest           : {manifest}")

    # Calcular HMAC-SHA256
    expected = hmac.new(
        secret.encode('utf-8'),
        manifest.encode('utf-8'),
        hashlib.sha256
    ).hexdigest()

    if not hmac.compare_digest(expected, v1):
        logger.error(f"❌ Assinatura inválida. Esperado: {expected} | Recebido: {v1}")
        return False

    logger.info("✅ Assinatura do webhook validada com sucesso")
    return True


def get_db_connection():
    """Cria conexão com PostgreSQL"""
    conn = psycopg2.connect(
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT', 5432),
        database=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        client_encoding='utf8'
    )
    return conn


@mercadopago_bp.route('/mercadopago/create-preference', methods=['POST'])
def create_preference():
    """
    Criar preferência de pagamento no Mercado Pago
    """
    try:
        data = request.get_json()

        logger.info("=" * 60)
        logger.info("📝 CRIANDO PREFERÊNCIA DE PAGAMENTO")
        logger.info("=" * 60)
        logger.info(f"Dados recebidos: {json.dumps(data, indent=2)}")

        # Validar dados obrigatórios
        if not data or 'plan' not in data or 'customer' not in data:
            logger.error("❌ Dados incompletos")
            return jsonify({
                'success': False,
                'error': 'Dados incompletos'
            }), 400

        plan = data['plan']
        customer = data['customer']
        extra_areas = data.get('extra_areas', 0)
        extra_areas_price = data.get('extra_areas_price', 0)
        extra_states = data.get('extra_states', 0)
        extra_states_price = data.get('extra_states_price', 0)
        total = data.get('total', plan['price'])
        selected_states = data.get('selected_states', [])
        selected_areas = data.get('selected_areas', [])

        logger.info(f"Plano: {plan['name']} (R$ {plan['price']})")
        logger.info(f"Cliente: {customer['name']} ({customer['email']})")
        logger.info(f"Estados extras: {extra_states} (R$ {extra_states_price})")
        logger.info(f"Áreas extras: {extra_areas} (R$ {extra_areas_price})")
        logger.info(f"Total: R$ {total}")

        # Gerar referência única
        reference_id = f"SEG-{datetime.now().strftime('%Y%m%d%H%M%S')}"
        logger.info(f"Referência gerada: {reference_id}")

        # Criar items para o Mercado Pago
        items = [
            {
                "title": plan['name'],
                "quantity": 1,
                "unit_price": float(plan['price']),
                "currency_id": "BRL"
            }
        ]

        if extra_states > 0 and extra_states_price > 0:
            items.append({
                "title": f"Estados Extras ({extra_states}x)",
                "quantity": 1,
                "unit_price": float(extra_states_price),
                "currency_id": "BRL"
            })
            logger.info(f"✅ Item de estados extras adicionado: {extra_states}x R$ 7,00 = R$ {extra_states_price}")

        # Adicionar áreas extras como item separado
        if extra_areas > 0 and extra_areas_price > 0:
            items.append({
                "title": f"Áreas Extras ({extra_areas}x)",
                "quantity": 1,
                "unit_price": float(extra_areas_price),
                "currency_id": "BRL"
            })
            logger.info(f"Item de áreas extras adicionado: {extra_areas}x R$ 7,00")

        # URLs de retorno
        frontend_url = os.getenv('FRONTEND_URL', 'http://localhost:3000')
        backend_url = os.getenv('BACKEND_URL', 'http://localhost:5000')

        logger.info(f"Frontend URL: {frontend_url}")
        logger.info(f"Backend URL: {backend_url}")

        # Criar preferência
        preference_data = {
            "items": items,
            "payer": {
                "name": customer['name'],
                "email": customer['email'],
                "identification": {
                    "type": "CPF",
                    "number": customer['cpf']
                },
                "phone": {
                    "number": customer['phone']
                }
            },
            "back_urls": {
                "success": f"{frontend_url}/payment/success",
                "failure": f"{frontend_url}/payment/failure",
                "pending": f"{frontend_url}/payment/pending"
            },
            "external_reference": reference_id,
            "statement_descriptor": "SEGLICIT",
            "notification_url": f"{backend_url}/api/mercadopago/webhook",
            "metadata": {
                "plan_id": plan['id'],
                "plan_name": plan['name'],
                "extra_states": extra_states,
                "extra_areas": extra_areas,
                "states": ",".join(selected_states),
                "areas": ",".join(selected_areas)
            }
        }

        logger.info("Criando preferência no Mercado Pago...")
        logger.info(f"Dados da preferência: {json.dumps(preference_data, indent=2)}")

        # Inicializar SDK do Mercado Pago
        sdk = mercadopago.SDK(os.getenv('MERCADOPAGO_ACCESS_TOKEN'))

        # Criar preferência
        preference_response = sdk.preference().create(preference_data)

        logger.info(f"Resposta do Mercado Pago: {json.dumps(preference_response, indent=2)}")

        # Verificar se a criação foi bem-sucedida
        if preference_response.get('status') not in [200, 201]:
            error_message = preference_response.get('response', {}).get('message', 'Erro desconhecido')
            logger.error(f"❌ Erro do Mercado Pago: {error_message}")
            return jsonify({
                'success': False,
                'error': f'Erro do Mercado Pago: {error_message}',
                'details': preference_response
            }), 400

        preference = preference_response.get("response")

        if not preference or 'id' not in preference:
            logger.error("❌ Preferência não retornou ID")
            return jsonify({
                'success': False,
                'error': 'Preferência inválida',
                'details': preference_response
            }), 500

        preference_id = preference['id']
        init_point = preference.get('init_point')
        sandbox_init_point = preference.get('sandbox_init_point')

        logger.info(f"✅ Preferência criada: {preference_id}")
        logger.info(f"Init Point: {init_point}")

        senha_hash = None
        if 'senha' in customer and customer['senha']:
            senha_hash = bcrypt.hashpw(
                customer['senha'].encode('utf-8'),
                bcrypt.gensalt()
            ).decode('utf-8')
            logger.info("✅ Senha hasheada com sucesso")
        else:
            logger.warning("⚠️ Senha não fornecida no checkout")

        # Salvar no banco de dados
        try:
            logger.info("Salvando pagamento no banco de dados...")

            conn = get_db_connection()
            cursor = conn.cursor()

            cursor.execute("""
                INSERT INTO payments (
                    reference_id, preference_id, status,
                    plan_id, plan_name, plan_price,
                    customer_name, customer_email, customer_cpf, customer_phone,
                    customer_empresa, customer_cnpj, customer_senha_hash,
                    extra_states, extra_states_price,
                    extra_areas, extra_areas_price, total_amount,
                    selected_states, selected_areas,
                    created_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            """, (
                reference_id,
                preference_id,
                'pending',
                plan['id'],
                plan['name'],
                plan['price'],
                customer['name'],
                customer['email'],
                customer['cpf'],
                customer['phone'],
                customer.get('empresa'),
                customer.get('cnpj'),
                senha_hash,
                extra_states,
                extra_states_price,
                extra_areas,
                extra_areas_price,
                total,
                json.dumps(selected_states),
                json.dumps(selected_areas)
            ))

            logger.info(f"✅ Payment criado:")
            logger.info(f"   Email: {customer['email']}")
            logger.info(f"   Nome: {customer['name']}")
            if customer.get('empresa'):
                logger.info(f"   Empresa: {customer.get('empresa')}")
            if customer.get('cnpj'):
                logger.info(f"   CNPJ: {customer.get('cnpj')}")
            if senha_hash:
                logger.info(f"   ✅ Senha salva (hasheada)")

            conn.commit()
            cursor.close()
            conn.close()

            logger.info("✅ Pagamento salvo no banco de dados")

        except Exception as e:
            logger.error(f"❌ Erro ao salvar no banco: {e}")
            logger.error(traceback.format_exc())

        logger.info("=" * 60)
        logger.info("✅ PREFERÊNCIA CRIADA COM SUCESSO!")
        logger.info("=" * 60)

        return jsonify({
            'success': True,
            'preference_id': preference_id,
            'init_point': init_point,
            'sandbox_init_point': sandbox_init_point,
            'reference_id': reference_id
        }), 200

    except Exception as e:
        logger.error("=" * 60)
        logger.error(f"❌ ERRO AO CRIAR PREFERÊNCIA: {e}")
        logger.error(traceback.format_exc())
        logger.error("=" * 60)
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


def _parse_checkout_payload(data):
    if not data or 'plan' not in data or 'customer' not in data:
        return None, 'Dados incompletos'

    plan = data['plan']
    customer = data['customer']
    payload = {
        'plan': plan,
        'customer': customer,
        'extra_areas': data.get('extra_areas', 0),
        'extra_areas_price': data.get('extra_areas_price', 0),
        'extra_states': data.get('extra_states', 0),
        'extra_states_price': data.get('extra_states_price', 0),
        'total': data.get('total', plan['price']),
        'selected_states': data.get('selected_states', []),
        'selected_areas': data.get('selected_areas', []),
        'terms_accepted': bool(data.get('terms_accepted')),
        'terms_version': (data.get('terms_version') or '').strip(),
    }
    return payload, None


CHECKOUT_TERMS_VERSION = '1.0-2026-07'


def _validate_terms(payload):
    if not payload.get('terms_accepted'):
        return 'É necessário aceitar os Termos de Contratação e a Política de Privacidade (LGPD).'
    version = payload.get('terms_version') or ''
    if version != CHECKOUT_TERMS_VERSION:
        return 'Versão dos termos desatualizada. Atualize a página do checkout e aceite novamente.'
    return None


def _save_pending_payment(reference_id, payload, external_id=None, billing_type='one_time'):
    plan = payload['plan']
    customer = payload['customer']

    senha_hash = None
    if customer.get('senha'):
        senha_hash = bcrypt.hashpw(
            customer['senha'].encode('utf-8'),
            bcrypt.gensalt(),
        ).decode('utf-8')

    conn = get_db_connection()
    cursor = conn.cursor()

    terms_record = {
        'terms_accepted': payload.get('terms_accepted'),
        'terms_version': payload.get('terms_version'),
        'terms_accepted_at': datetime.utcnow().isoformat() + 'Z',
    }

    cursor.execute("""
        INSERT INTO payments (
            reference_id, preference_id, status,
            plan_id, plan_name, plan_price,
            customer_name, customer_email, customer_cpf, customer_phone,
            customer_empresa, customer_cnpj, customer_senha_hash,
            extra_states, extra_states_price,
            extra_areas, extra_areas_price, total_amount,
            selected_states, selected_areas,
            mp_preapproval_id, billing_type, payment_data,
            created_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
    """, (
        reference_id,
        external_id,
        'pending',
        plan['id'],
        plan['name'],
        plan['price'],
        customer['name'],
        customer['email'],
        customer['cpf'],
        customer['phone'],
        customer.get('empresa'),
        customer.get('cnpj'),
        senha_hash,
        payload['extra_states'],
        payload['extra_states_price'],
        payload['extra_areas'],
        payload['extra_areas_price'],
        payload['total'],
        json.dumps(payload['selected_states']),
        json.dumps(payload['selected_areas']),
        external_id if billing_type == 'subscription' else None,
        billing_type,
        json.dumps(terms_record),
    ))
    conn.commit()
    cursor.close()
    conn.close()


@mercadopago_bp.route('/mercadopago/create-subscription', methods=['POST'])
def create_subscription():
    """Cria assinatura recorrente mensal no Mercado Pago (preapproval)."""
    try:
        data = request.get_json()
        payload, error = _parse_checkout_payload(data)
        if error:
            return jsonify({'success': False, 'error': error}), 400

        terms_error = _validate_terms(payload)
        if terms_error:
            return jsonify({'success': False, 'error': terms_error}), 400

        plan = payload['plan']
        customer = payload['customer']
        total = payload['total']
        reference_id = f"SEG-{datetime.now().strftime('%Y%m%d%H%M%S')}"

        frontend_url = os.getenv('FRONTEND_URL', 'http://localhost:3000')
        backend_url = os.getenv('BACKEND_URL', 'http://localhost:5000')

        preapproval_data = {
            'reason': f"Seglicit - {plan['name']}",
            'external_reference': reference_id,
            'payer_email': customer['email'].strip().lower(),
            'back_url': f"{frontend_url}/payment/success?external_reference={reference_id}",
            'notification_url': f"{backend_url}/api/mercadopago/webhook",
            'auto_recurring': {
                'frequency': 1,
                'frequency_type': 'months',
                'transaction_amount': round(float(total), 2),
                'currency_id': 'BRL',
            },
            'payment_methods_allowed': {
                'payment_types': [{'id': 'credit_card'}],
            },
        }

        from src.services.email_validation import validate_checkout_email
        email_check = validate_checkout_email(customer['email'])
        if not email_check['valid']:
            return jsonify({
                'success': False,
                'error': email_check['message'],
            }), 400

        logger.info('Criando assinatura MP ref=%s total=R$ %s', reference_id, total)
        preapproval_response = sdk.preapproval().create(preapproval_data)

        if preapproval_response.get('status') not in [200, 201]:
            error_message = preapproval_response.get('response', {}).get('message', 'Erro desconhecido')
            logger.error('Erro MP preapproval: %s', error_message)
            return jsonify({
                'success': False,
                'error': f'Erro do Mercado Pago: {error_message}',
                'details': preapproval_response,
            }), 400

        preapproval = preapproval_response.get('response') or {}
        preapproval_id = preapproval.get('id')
        access_token = os.getenv('MERCADOPAGO_ACCESS_TOKEN', '')
        use_sandbox = access_token.startswith('TEST-')
        init_point = (
            preapproval.get('sandbox_init_point') if use_sandbox else preapproval.get('init_point')
        ) or preapproval.get('init_point') or preapproval.get('sandbox_init_point')

        if not preapproval_id or not init_point:
            return jsonify({
                'success': False,
                'error': 'Resposta inválida do Mercado Pago',
                'details': preapproval_response,
            }), 500

        try:
            _save_pending_payment(reference_id, payload, external_id=preapproval_id, billing_type='subscription')
        except Exception as db_err:
            logger.error('Erro ao salvar assinatura pendente: %s', db_err)
            logger.error(traceback.format_exc())

        return jsonify({
            'success': True,
            'preapproval_id': preapproval_id,
            'init_point': init_point,
            'reference_id': reference_id,
        }), 200

    except Exception as e:
        logger.error('Erro ao criar assinatura: %s', e)
        logger.error(traceback.format_exc())
        return jsonify({'success': False, 'error': str(e)}), 500


def _handle_payment_notification(payment_id):
    payment_info = sdk.payment().get(payment_id)
    if payment_info['status'] != 200:
        logger.error('Erro ao consultar pagamento: %s', payment_info)
        return jsonify({'success': False, 'error': 'Payment not found'}), 404

    payment = payment_info['response']
    reference_id = payment.get('external_reference')
    if not reference_id:
        return jsonify({'success': False, 'error': 'No reference'}), 400

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
        UPDATE payments
        SET status = %s,
            payment_id = %s,
            payment_data = %s,
            updated_at = NOW()
        WHERE reference_id = %s
    """, (
        payment['status'],
        str(payment_id),
        json.dumps(payment),
        reference_id,
    ))

    cursor.execute(
        "SELECT billing_type FROM payments WHERE reference_id = %s",
        (reference_id,),
    )
    billing_row = cursor.fetchone()
    billing_type = billing_row[0] if billing_row else 'one_time'

    if billing_type == 'subscription':
        if payment['status'] == 'rejected':
            detail = payment.get('status_detail')
            logger.error(
                'Assinatura recusada ref=%s payment_id=%s detail=%s payer_email=%s',
                reference_id,
                payment_id,
                detail,
                (payment.get('payer') or {}).get('email'),
            )
        elif payment['status'] == 'approved':
            # Não depender só do webhook subscription_authorized_payment pra
            # ativar — se ele atrasar/falhar/não chegar, a cobrança acontece
            # no MP mas a assinatura nunca é criada no nosso banco. Ativa já
            # aqui também; activate_subscription_from_reference é idempotente
            # (upsert por user_id), então não duplica se o outro webhook
            # também chamar depois.
            activate_subscription_from_reference(cursor, reference_id, mp_payment_id=payment_id)
        conn.commit()
        cursor.close()
        conn.close()
        logger.info('Pagamento de assinatura registrado ref=%s', reference_id)
        return jsonify({'success': True}), 200

    if payment['status'] == 'approved':
        activate_subscription_from_reference(cursor, reference_id, mp_payment_id=payment_id)
        conn.commit()
    else:
        conn.commit()

    cursor.close()
    conn.close()
    return jsonify({'success': True}), 200


def _handle_preapproval_notification(preapproval_id):
    preapproval_info = sdk.preapproval().get(preapproval_id)
    if preapproval_info.get('status') != 200:
        logger.error('Erro ao consultar preapproval: %s', preapproval_info)
        return jsonify({'success': False, 'error': 'Preapproval not found'}), 404

    preapproval = preapproval_info.get('response') or {}
    conn = get_db_connection()
    cursor = conn.cursor()
    update_subscription_by_preapproval_status(cursor, preapproval)
    conn.commit()
    cursor.close()
    conn.close()
    return jsonify({'success': True}), 200


def _handle_authorized_payment_notification(auth_payment_id):
    access_token = os.getenv('MERCADOPAGO_ACCESS_TOKEN')
    authorized_payment = fetch_authorized_payment(auth_payment_id, access_token)
    if not authorized_payment:
        return jsonify({'success': False, 'error': 'Authorized payment not found'}), 404

    conn = get_db_connection()
    cursor = conn.cursor()
    process_authorized_payment_notification(cursor, authorized_payment)
    conn.commit()
    cursor.close()
    conn.close()
    return jsonify({'success': True}), 200


@mercadopago_bp.route('/mercadopago/webhook', methods=['POST'])
def webhook():
    """
    Webhook para receber notificações do Mercado Pago
    """
    try:
        # Verificar assinatura antes de processar
        if not _verify_webhook_signature(request):
            logger.error("❌ Webhook rejeitado: assinatura inválida")
            return jsonify({'error': 'Invalid signature'}), 401

        # LOG 1: Webhook foi chamado
        logger.info("=" * 60)
        logger.info("🔔 WEBHOOK RECEBIDO!")
        logger.info("=" * 60)

        # LOG 2: Headers da requisição
        logger.info("Headers:")
        for header, value in request.headers.items():
            logger.info(f"  {header}: {value}")

        # LOG 3: Corpo da requisição
        data = request.get_json()
        logger.info("Body:")
        logger.info(json.dumps(data, indent=2))

        notification_type = data.get('type')
        action = data.get('action')
        resource_id = data.get('data', {}).get('id')
        logger.info('Tipo=%s action=%s id=%s', notification_type, action, resource_id)

        if notification_type == 'payment' and resource_id:
            return _handle_payment_notification(resource_id)

        if notification_type == 'subscription_preapproval' and resource_id:
            return _handle_preapproval_notification(resource_id)

        if notification_type == 'subscription_authorized_payment' and resource_id:
            return _handle_authorized_payment_notification(resource_id)

        logger.info("Tipo de notificação '%s' ignorado", notification_type)
        return jsonify({'success': True, 'message': 'Ignored'}), 200

    except Exception as e:
        # LOG 16: Erro geral
        logger.error("=" * 60)
        logger.error(f"❌ ERRO NO WEBHOOK: {e}")
        logger.error(traceback.format_exc())
        logger.error("=" * 60)
        return jsonify({'success': False, 'error': str(e)}), 500


@mercadopago_bp.route('/mercadopago/checkout-status', methods=['GET'])
def checkout_status():
    """Diagnóstico de checkout/assinatura por reference_id ou preference_id."""
    reference_id = request.args.get('reference_id') or request.args.get('reference')
    preference_id = request.args.get('preference_id')
    preapproval_id = request.args.get('preapproval_id')

    conn = get_db_connection()
    cursor = conn.cursor()
    row = None

    if reference_id:
        cursor.execute(
            """
            SELECT reference_id, customer_email, status, total_amount, mp_preapproval_id,
                   preference_id, payment_id, billing_type, created_at
            FROM payments
            WHERE reference_id = %s
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (reference_id,),
        )
        row = cursor.fetchone()
    elif preference_id or preapproval_id:
        lookup_id = preapproval_id or preference_id
        cursor.execute(
            """
            SELECT reference_id, customer_email, status, total_amount, mp_preapproval_id,
                   preference_id, payment_id, billing_type, created_at
            FROM payments
            WHERE mp_preapproval_id = %s OR preference_id = %s
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (lookup_id, lookup_id),
        )
        row = cursor.fetchone()

    if not row:
        cursor.close()
        conn.close()
        return jsonify({'success': False, 'error': 'Registro não encontrado'}), 404

    subscription_row = None
    cursor.execute(
        """
        SELECT status, plan_name, current_period_end
        FROM subscriptions
        WHERE payment_reference = %s
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (row[0],),
    )
    subscription_row = cursor.fetchone()

    cursor.close()
    conn.close()

    payment_record = {
        'reference_id': row[0],
        'customer_email': row[1],
        'status': row[2],
        'total_amount': float(row[3]) if row[3] is not None else None,
        'mp_preapproval_id': row[4],
        'preference_id': row[5],
        'payment_id': row[6],
        'billing_type': row[7],
        'created_at': row[8].isoformat() if row[8] else None,
    }

    mp_preapproval = None
    mp_payment = None
    preapproval_lookup_id = payment_record.get('mp_preapproval_id') or preapproval_id

    if preapproval_lookup_id:
        preapproval_info = sdk.preapproval().get(preapproval_lookup_id)
        if preapproval_info.get('status') == 200:
            mp_preapproval = preapproval_info.get('response')

    if payment_record.get('payment_id'):
        payment_info = sdk.payment().get(payment_record['payment_id'])
        if payment_info.get('status') == 200:
            payment = payment_info['response']
            mp_payment = {
                'id': payment.get('id'),
                'status': payment.get('status'),
                'status_detail': payment.get('status_detail'),
                'status_detail_message': _payment_status_detail_message(payment.get('status_detail')),
                'payer_email': (payment.get('payer') or {}).get('email'),
                'payment_method_id': payment.get('payment_method_id'),
                'payment_type_id': payment.get('payment_type_id'),
            }

    tips = []
    if mp_payment and mp_payment.get('status') == 'rejected':
        detail = mp_payment.get('status_detail')
        checkout_email = (payment_record.get('customer_email') or '').lower()
        payer_email = (mp_payment.get('payer_email') or '').lower()
        if checkout_email and payer_email and checkout_email != payer_email:
            tips.append(
                f'Email divergente: checkout={checkout_email} | mercado_pago={payer_email}. '
                'Use o mesmo email nos dois lugares.'
            )
        if detail == 'cc_rejected_high_risk':
            tips.append(
                'Recusado por segurança. Tente no navegador normal (sem anônimo), '
                'com cartão de crédito habitual e mesmo dispositivo de compras online.'
            )
        elif detail == 'cc_rejected_insufficient_amount':
            tips.append('Limite ou saldo insuficiente no cartão de crédito.')
        elif detail:
            tips.append(mp_payment.get('status_detail_message') or detail)

    if mp_preapproval and mp_preapproval.get('status') == 'pending':
        tips.append(
            f'Assinatura pendente no MP. Ao pagar, use o email: {payment_record.get("customer_email")}'
        )

    subscription_status = subscription_row[0] if subscription_row else None
    subscription_active = subscription_status == 'active'
    payment_db_status = payment_record.get('status') or ''
    platform_ready = subscription_active and payment_db_status in (
        'approved',
        'authorized',
        'active',
    )

    return jsonify({
        'success': True,
        'payment_record': payment_record,
        'subscription_status': subscription_status,
        'subscription_plan_name': subscription_row[1] if subscription_row else None,
        'subscription_active': subscription_active,
        'platform_ready': platform_ready,
        'mp_preapproval_status': (mp_preapproval or {}).get('status'),
        'mp_preapproval_payer_email': (mp_preapproval or {}).get('payer_email'),
        'mp_payment': mp_payment,
        'tips': tips,
    }), 200


@mercadopago_bp.route('/mercadopago/reprocess-subscription', methods=['POST'])
def reprocess_subscription():
    """
    Reativa manualmente uma assinatura a partir de um reference_id (ou
    payment_id, se não souber o reference_id) — usado quando o pagamento foi
    aprovado no Mercado Pago mas o webhook que deveria ter ativado a
    assinatura no nosso banco não chegou ou falhou (ver activate_subscription_from_reference,
    idempotente e seguro de rodar de novo). Protegido por senha própria.
    """
    admin_secret = os.getenv('MERCADOPAGO_RECONCILE_SECRET')
    if not admin_secret or request.headers.get('X-Admin-Secret') != admin_secret:
        return jsonify({'success': False, 'error': 'Não autorizado'}), 401

    data = request.get_json() or {}
    reference_id = (data.get('reference_id') or '').strip()
    payment_id = (data.get('payment_id') or '').strip()

    if not reference_id and payment_id:
        payment_info = sdk.payment().get(payment_id)
        if payment_info.get('status') == 200:
            reference_id = (payment_info['response'].get('external_reference') or '').strip()

    if not reference_id:
        return jsonify({
            'success': False,
            'error': 'Informe reference_id, ou um payment_id válido com external_reference vinculado.',
        }), 400

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT mp_preapproval_id FROM payments WHERE reference_id = %s", (reference_id,))
    row = cursor.fetchone()
    mp_preapproval_id = row[0] if row else None

    ok = activate_subscription_from_reference(
        cursor,
        reference_id,
        mp_preapproval_id=mp_preapproval_id,
        mp_payment_id=payment_id or None,
    )
    conn.commit()
    cursor.close()
    conn.close()

    if not ok:
        return jsonify({'success': False, 'error': f'reference_id {reference_id} não encontrado em payments'}), 404

    return jsonify({'success': True, 'reference_id': reference_id}), 200


@mercadopago_bp.route('/mercadopago/payment/<payment_id>', methods=['GET'])
def get_payment_status(payment_id):
    """
    Consultar status de um pagamento
    """
    try:
        logger.info(f"Consultando status do pagamento: {payment_id}")

        sdk = mercadopago.SDK(os.getenv('MERCADOPAGO_ACCESS_TOKEN'))
        payment_info = sdk.payment().get(payment_id)

        if payment_info['status'] == 200:
            payment = payment_info['response']

            logger.info(f"Status: {payment['status']}")
            logger.info(f"Valor: R$ {payment['transaction_amount']}")

            return jsonify({
                'success': True,
                'payment': {
                    'id': payment['id'],
                    'status': payment['status'],
                    'status_detail': payment.get('status_detail'),
                    'status_detail_message': _payment_status_detail_message(payment.get('status_detail')),
                    'amount': payment['transaction_amount'],
                    'reference': payment.get('external_reference'),
                    'payer_email': (payment.get('payer') or {}).get('email'),
                    'date_created': payment.get('date_created'),
                    'date_approved': payment.get('date_approved'),
                }
            }), 200
        else:
            logger.error(f"Erro ao consultar pagamento: {payment_info}")
            return jsonify({
                'success': False,
                'error': 'Payment not found'
            }), 404

    except Exception as e:
        logger.error(f"Erro ao consultar pagamento: {e}")
        logger.error(traceback.format_exc())
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@mercadopago_bp.route('/mercadopago/test-email', methods=['POST'])
def test_payment_email():
    """Endpoint temporário para testar email de confirmação sem fazer pagamento real."""
    try:
        data = request.get_json()
        email = data.get('email')
        if not email:
            return jsonify({'success': False, 'error': 'email obrigatório'}), 400

        from src.services.email_service import send_payment_confirmation
        send_payment_confirmation(
            customer_email=email,
            customer_name=data.get('name', 'Usuário Teste'),
            plan_name=data.get('plan', 'Básico'),
            selected_states=data.get('states', ['SP', 'RJ']),
            selected_areas=data.get('areas', ['Construção Civil'])
        )
        return jsonify({'success': True, 'message': f'Email de teste enviado para {email}'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500