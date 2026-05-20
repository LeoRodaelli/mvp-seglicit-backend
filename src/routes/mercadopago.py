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

        # LOG 4: Tipo de notificação
        notification_type = data.get('type')
        logger.info(f"Tipo de notificação: {notification_type}")

        # Processar apenas notificações de pagamento
        if notification_type == 'payment':
            payment_id = data.get('data', {}).get('id')

            # LOG 5: Payment ID recebido
            logger.info(f"Payment ID: {payment_id}")

            if not payment_id:
                logger.error("❌ Payment ID não encontrado no webhook")
                return jsonify({'success': False, 'error': 'Payment ID missing'}), 400

            # LOG 6: Consultando pagamento no MP
            logger.info(f"Consultando pagamento {payment_id} no Mercado Pago...")

            sdk = mercadopago.SDK(os.getenv('MERCADOPAGO_ACCESS_TOKEN'))
            payment_info = sdk.payment().get(payment_id)

            # LOG 7: Resposta do MP
            logger.info("Resposta do Mercado Pago:")
            logger.info(json.dumps(payment_info, indent=2))

            if payment_info['status'] == 200:
                payment = payment_info['response']

                # LOG 8: Dados do pagamento
                logger.info(f"Status do pagamento: {payment['status']}")
                logger.info(f"Valor: R$ {payment['transaction_amount']}")
                logger.info(f"Referência: {payment.get('external_reference')}")

                # Atualizar banco de dados
                reference_id = payment.get('external_reference')

                if reference_id:
                    # LOG 9: Atualizando banco
                    logger.info(f"Atualizando banco para referência: {reference_id}")

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
                        reference_id
                    ))

                    updated_rows = cursor.rowcount

                    # LOG 10: Resultado da atualização
                    logger.info(f"Linhas atualizadas: {updated_rows}")

                    # Se pagamento aprovado, criar/atualizar assinatura
                    if payment['status'] == 'approved':
                        logger.info("✅ Pagamento APROVADO! Criando assinatura...")

                        # Buscar dados do pagamento
                        cursor.execute("""
                            SELECT customer_email, plan_id, plan_name, 
                                   selected_states, selected_areas,
                                   extra_states, extra_states_price,
                                    extra_areas, extra_areas_price
                            FROM payments
                            WHERE reference_id = %s
                        """, (reference_id,))

                        payment_data = cursor.fetchone()

                        if payment_data:
                            # LOG 11: Dados para criar assinatura
                            logger.info(f"Dados do pagamento: {payment_data}")
                            logger.info(f"Payment encontrado: {payment_data[0]}")
                            logger.info(f"Estados extras: {payment_data[5]} (R$ {payment_data[6]})")  # ✅ ADICIONAR
                            logger.info(f"Áreas extras: {payment_data[7]} (R$ {payment_data[8]})")

                            # Buscar user_id pelo email
                            cursor.execute("""
                                SELECT id FROM users WHERE email = %s
                            """, (payment_data[0],))

                            user_result = cursor.fetchone()
                            user_id = user_result[0] if user_result else None

                            # LOG 12: User ID encontrado
                            logger.info(f"User ID: {user_id}")

                            # ✨ CORRIGIDO: Se usuário não existe, criar automaticamente
                            if not user_id:
                                logger.info(f"⚠️ Usuário não encontrado. Criando usuário para: {payment_data[0]}")

                                # ✅ CORREÇÃO 1: Buscar TODOS os dados do cliente (incluindo senha)
                                cursor.execute("""
                                    SELECT customer_name, customer_cpf, customer_phone,
                                           customer_empresa, customer_cnpj, customer_senha_hash
                                    FROM payments
                                    WHERE reference_id = %s
                                """, (reference_id,))

                                customer_data = cursor.fetchone()

                                if customer_data:
                                    customer_name = customer_data[0]
                                    customer_cpf = customer_data[1]
                                    customer_phone = customer_data[2]
                                    customer_empresa = customer_data[3]  # ✅ NOVO
                                    customer_cnpj = customer_data[4]  # ✅ NOVO
                                    customer_senha_hash = customer_data[5]  # ✅ NOVO

                                    # ✅ CORREÇÃO 2: Gerar username a partir do email
                                    email = payment_data[0]
                                    username = email.split('@')[0]

                                    # Verificar se username já existe
                                    cursor.execute("SELECT COUNT(*) FROM users WHERE username = %s", (username,))
                                    count = cursor.fetchone()[0]
                                    if count > 0:
                                        username = f"{username}_{count + 1}"

                                    logger.info(f"   Username gerado: {username}")

                                    # ✅ CORREÇÃO 3: Se senha não existe, gerar temporária
                                    if not customer_senha_hash:
                                        import secrets
                                        import bcrypt

                                        temp_password = secrets.token_urlsafe(12)
                                        customer_senha_hash = bcrypt.hashpw(
                                            temp_password.encode('utf-8'),
                                            bcrypt.gensalt()
                                        ).decode('utf-8')
                                        logger.warning(f"⚠️ Senha não encontrada. Gerada senha temporária.")
                                        logger.warning(f"   Usuário deve redefinir senha no primeiro acesso.")

                                    # ✅ CORREÇÃO 4: Criar usuário COM TODOS OS CAMPOS OBRIGATÓRIOS
                                    cursor.execute("""
                                        INSERT INTO users (
                                            username, email, password_hash, full_name, 
                                            phone, company_name, cnpj_cpf,
                                            created_at, updated_at
                                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                                        RETURNING id
                                    """, (
                                        username,  # ✅ NOVO - OBRIGATÓRIO
                                        email,
                                        customer_senha_hash,  # ✅ NOVO - OBRIGATÓRIO
                                        customer_name,
                                        customer_phone,
                                        customer_empresa,  # ✅ NOVO
                                        customer_cnpj if customer_cnpj else customer_cpf  # ✅ CNPJ ou CPF
                                    ))

                                    user_id = cursor.fetchone()[0]
                                    logger.info(f"✅ Usuário criado com ID: {user_id}")
                                    logger.info(f"   Username: {username}")
                                    logger.info(f"   Email: {email}")
                                    logger.info(f"   Nome: {customer_name}")
                                    if customer_empresa:
                                        logger.info(f"   Empresa: {customer_empresa}")
                                    if customer_cnpj:
                                        logger.info(f"   CNPJ: {customer_cnpj}")

                                else:
                                    logger.error(f"❌ Não foi possível obter dados do cliente")

                            if user_id:
                                # Criar ou atualizar assinatura
                                cursor.execute("""
                                    INSERT INTO subscriptions (
                                        user_id, plan_id, plan_name, status,
                                        selected_states, selected_areas,
                                        payment_reference, start_date, created_at
                                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                                    ON CONFLICT (user_id) 
                                    DO UPDATE SET
                                        plan_id = EXCLUDED.plan_id,
                                        plan_name = EXCLUDED.plan_name,
                                        status = EXCLUDED.status,
                                        selected_states = EXCLUDED.selected_states,
                                        selected_areas = EXCLUDED.selected_areas,
                                        payment_reference = EXCLUDED.payment_reference,
                                        updated_at = NOW()
                                """, (
                                    user_id,
                                    payment_data[1],  # plan_id
                                    payment_data[2],  # plan_name
                                    'active',
                                    json.dumps(payment_data[3]) if isinstance(payment_data[3], list) else payment_data[
                                        3],  # selected_states
                                    json.dumps(payment_data[4]) if isinstance(payment_data[4], list) else payment_data[
                                        4],  # selected_areas
                                    reference_id
                                ))

                                # LOG 13: Assinatura criada
                                logger.info("✅ Assinatura criada/atualizada com sucesso!")
                            else:
                                logger.warning(f"⚠️ Usuário não encontrado para email: {payment_data[0]}")
                        else:
                            logger.warning(f"⚠️ Dados do pagamento não encontrados para ref: {reference_id}")

                    conn.commit()
                    cursor.close()
                    conn.close()

                    # LOG 14: Sucesso final
                    logger.info("=" * 60)
                    logger.info("✅ WEBHOOK PROCESSADO COM SUCESSO!")
                    logger.info("=" * 60)

                    return jsonify({'success': True}), 200
                else:
                    logger.error("❌ Referência externa não encontrada no pagamento")
                    return jsonify({'success': False, 'error': 'No reference'}), 400
            else:
                logger.error(f"❌ Erro ao consultar pagamento: {payment_info}")
                return jsonify({'success': False, 'error': 'Payment not found'}), 404
        else:
            # LOG 15: Tipo de notificação ignorado
            logger.info(f"ℹ️ Tipo de notificação '{notification_type}' ignorado")
            return jsonify({'success': True, 'message': 'Ignored'}), 200

    except Exception as e:
        # LOG 16: Erro geral
        logger.error("=" * 60)
        logger.error(f"❌ ERRO NO WEBHOOK: {e}")
        logger.error(traceback.format_exc())
        logger.error("=" * 60)
        return jsonify({'success': False, 'error': str(e)}), 500


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
                    'amount': payment['transaction_amount'],
                    'reference': payment.get('external_reference'),
                    'date_created': payment.get('date_created'),
                    'date_approved': payment.get('date_approved')
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