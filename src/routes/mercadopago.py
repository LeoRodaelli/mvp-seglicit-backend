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
from datetime import datetime
from dotenv import load_dotenv
import psycopg2
import psycopg2.extras

load_dotenv()

# Configure logging
logger = logging.getLogger(__name__)

# Criar Blueprint
mercadopago_bp = Blueprint('mercadopago', __name__)

# Inicializar SDK do Mercado Pago
sdk = mercadopago.SDK(os.getenv('MERCADOPAGO_ACCESS_TOKEN'))


def get_db_connection():
    """Cria conexão direta com PostgreSQL"""
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


@mercadopago_bp.route('/mercadopago/create-preference', methods=['POST'])
def create_preference():
    """
    Cria preferência de pagamento no Mercado Pago

    Body esperado:
    {
        "plan": {
            "id": "basico",
            "name": "Plano Básico",
            "price": 27.99
        },
        "extra_areas": 2,
        "extra_areas_price": 14.00,
        "total": 41.99,
        "customer": {
            "name": "João Silva",
            "email": "joao@email.com",
            "cpf": "12345678909",
            "phone": "11999999999"
        },
        "selected_states": ["SP", "RJ"],
        "selected_areas": ["Construção Civil", "Saúde", "Educação"]
    }
    """
    try:
        data = request.get_json()

        # Validar dados obrigatórios
        if not data or 'plan' not in data or 'customer' not in data:
            return jsonify({
                'success': False,
                'error': 'Dados incompletos'
            }), 400

        plan = data['plan']
        customer = data['customer']
        extra_areas = data.get('extra_areas', 0)
        extra_areas_price = data.get('extra_areas_price', 0)
        total = data.get('total', plan['price'])
        selected_states = data.get('selected_states', [])
        selected_areas = data.get('selected_areas', [])

        # Gerar referência única
        reference_id = f"SEG-{datetime.now().strftime('%Y%m%d%H%M%S')}"

        # Criar items para o Mercado Pago
        items = [
            {
                "title": plan['name'],
                "quantity": 1,
                "unit_price": float(plan['price']),
                "currency_id": "BRL"
            }
        ]

        # Adicionar áreas extras como item separado
        if extra_areas > 0 and extra_areas_price > 0:
            items.append({
                "title": f"Áreas Extras ({extra_areas}x)",
                "quantity": 1,
                "unit_price": float(extra_areas_price),
                "currency_id": "BRL"
            })

        # URLs de retorno
        frontend_url = os.getenv('FRONTEND_URL', 'http://localhost:3000')

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
            "notification_url": f"{os.getenv('BACKEND_URL', 'http://localhost:5000')}/api/mercadopago/webhook",
            "metadata": {
                "plan_id": plan['id'],
                "plan_name": plan['name'],
                "extra_areas": extra_areas,
                "states": ",".join(selected_states),
                "areas": ",".join(selected_areas)
            }
        }

        # Criar preferência no Mercado Pago
        logger.info(f"Criando preferência com dados: {json.dumps(preference_data, indent=2)}")
        preference_response = sdk.preference().create(preference_data)

        # Log da resposta completa para debug
        logger.info(f"Resposta do Mercado Pago: {json.dumps(preference_response, indent=2)}")

        # Verificar se houve erro
        if preference_response.get('status') not in [200, 201]:
            error_message = preference_response.get('response', {}).get('message', 'Erro desconhecido')
            logger.error(f"Erro ao criar preferência: {error_message}")
            return jsonify({
                'success': False,
                'error': f'Erro do Mercado Pago: {error_message}',
                'details': preference_response
            }), 400

        # Extrair preferência da resposta
        preference = preference_response.get("response")
        if not preference or 'id' not in preference:
            logger.error(f"Resposta inválida do Mercado Pago: {preference_response}")
            return jsonify({
                'success': False,
                'error': 'Resposta inválida do Mercado Pago',
                'details': preference_response
            }), 500

        # Salvar no banco de dados
        conn = get_db_connection()
        if conn:
            try:
                cursor = conn.cursor()

                # Inserir na tabela payments
                cursor.execute("""
                    INSERT INTO payments (
                        reference_id,
                        preference_id,
                        plan_id,
                        plan_name,
                        plan_price,
                        extra_areas,
                        extra_areas_price,
                        total_amount,
                        customer_name,
                        customer_email,
                        customer_cpf,
                        customer_phone,
                        selected_states,
                        selected_areas,
                        status,
                        created_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    reference_id,
                    preference['id'],
                    plan['id'],
                    plan['name'],
                    plan['price'],
                    extra_areas,
                    extra_areas_price,
                    total,
                    customer['name'],
                    customer['email'],
                    customer['cpf'],
                    customer['phone'],
                    json.dumps(selected_states),
                    json.dumps(selected_areas),
                    'pending',
                    datetime.now()
                ))

                conn.commit()
                cursor.close()
                conn.close()

                logger.info(f"Preferência criada: {reference_id}")

            except Exception as e:
                logger.error(f"Erro ao salvar no banco: {e}")
                conn.rollback()
                conn.close()

        # Retornar dados para o frontend
        return jsonify({
            'success': True,
            'preference_id': preference['id'],
            'init_point': preference['init_point'],  # URL para desktop
            'sandbox_init_point': preference.get('sandbox_init_point'),  # URL para sandbox
            'reference_id': reference_id
        }), 200

    except Exception as e:
        logger.error(f"Erro ao criar preferência: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@mercadopago_bp.route('/mercadopago/webhook', methods=['POST'])
def webhook():
    """
    Recebe notificações do Mercado Pago sobre mudanças de status
    """
    try:
        data = request.get_json()

        logger.info(f"Webhook recebido: {json.dumps(data)}")

        # Mercado Pago envia o tipo de notificação
        if data.get('type') == 'payment':
            payment_id = data['data']['id']

            # Buscar informações do pagamento
            payment_info = sdk.payment().get(payment_id)
            payment = payment_info['response']

            # Extrair dados relevantes
            status = payment['status']
            external_reference = payment.get('external_reference')

            logger.info(f"Pagamento {payment_id} - Status: {status} - Ref: {external_reference}")

            # Atualizar no banco de dados
            conn = get_db_connection()
            if conn:
                try:
                    cursor = conn.cursor()

                    # Atualizar status do pagamento
                    cursor.execute("""
                        UPDATE payments 
                        SET status = %s,
                            payment_id = %s,
                            payment_data = %s,
                            updated_at = %s
                        WHERE reference_id = %s
                    """, (
                        status,
                        payment_id,
                        json.dumps(payment),
                        datetime.now(),
                        external_reference
                    ))

                    # Se pagamento aprovado, criar/atualizar assinatura
                    if status == 'approved':
                        # Buscar dados do pagamento
                        cursor.execute("""
                            SELECT customer_email, plan_id, plan_name, 
                                   selected_states, selected_areas
                            FROM payments
                            WHERE reference_id = %s
                        """, (external_reference,))

                        payment_data = cursor.fetchone()

                        if payment_data:
                            customer_email = payment_data[0]
                            plan_id = payment_data[1]
                            plan_name = payment_data[2]
                            selected_states = payment_data[3]
                            selected_areas = payment_data[4]

                            # Buscar user_id pelo email
                            cursor.execute("""
                                SELECT id FROM users WHERE email = %s
                            """, (customer_email,))

                            user_result = cursor.fetchone()

                            if user_result:
                                user_id = user_result[0]

                                # Criar ou atualizar assinatura
                                cursor.execute("""
                                    INSERT INTO subscriptions (
                                        user_id,
                                        plan_id,
                                        plan_name,
                                        status,
                                        selected_states,
                                        selected_areas,
                                        payment_reference,
                                        start_date,
                                        created_at
                                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                                    ON CONFLICT (user_id) 
                                    DO UPDATE SET
                                        plan_id = EXCLUDED.plan_id,
                                        plan_name = EXCLUDED.plan_name,
                                        status = EXCLUDED.status,
                                        selected_states = EXCLUDED.selected_states,
                                        selected_areas = EXCLUDED.selected_areas,
                                        payment_reference = EXCLUDED.payment_reference,
                                        start_date = EXCLUDED.start_date,
                                        updated_at = %s
                                """, (
                                    user_id,
                                    plan_id,
                                    plan_name,
                                    'active',
                                    selected_states,
                                    selected_areas,
                                    external_reference,
                                    datetime.now(),
                                    datetime.now(),
                                    datetime.now()
                                ))

                                logger.info(f"Assinatura criada/atualizada para user_id: {user_id}")

                    conn.commit()
                    cursor.close()
                    conn.close()

                    logger.info(f"Banco atualizado para referência: {external_reference}")

                except Exception as e:
                    logger.error(f"Erro ao atualizar banco: {e}")
                    conn.rollback()
                    conn.close()

        return jsonify({'success': True}), 200

    except Exception as e:
        logger.error(f"Erro no webhook: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@mercadopago_bp.route('/mercadopago/payment-status/<reference_id>', methods=['GET'])
def get_payment_status(reference_id):
    """
    Consulta status de um pagamento pela referência
    """
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({
                'success': False,
                'error': 'Erro de conexão com banco'
            }), 500

        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cursor.execute("""
            SELECT reference_id, plan_name, total_amount, status, 
                   customer_name, customer_email, created_at, updated_at
            FROM payments
            WHERE reference_id = %s
        """, (reference_id,))

        payment = cursor.fetchone()
        cursor.close()
        conn.close()

        if not payment:
            return jsonify({
                'success': False,
                'error': 'Pagamento não encontrado'
            }), 404

        return jsonify({
            'success': True,
            'payment': dict(payment)
        }), 200

    except Exception as e:
        logger.error(f"Erro ao consultar status: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@mercadopago_bp.route('/health', methods=['GET'])
def health_check():
    """Health check do serviço"""
    return jsonify({
        'status': 'healthy',
        'service': 'mercadopago-api'
    }), 200