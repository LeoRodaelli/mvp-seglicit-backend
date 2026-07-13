# -*- coding: utf-8 -*-
"""Lógica de assinaturas recorrentes (Mercado Pago preapproval)."""

import json
import logging
import secrets
from datetime import date, timedelta

import bcrypt
import requests

logger = logging.getLogger(__name__)

ACTIVE_SUBSCRIPTION_WHERE = """
    status = 'active'
    AND (current_period_end IS NULL OR current_period_end >= CURRENT_DATE)
"""

SUBSCRIPTION_PERIOD_DAYS = 30


def is_subscription_active(row):
    if not row or row.get('status') != 'active':
        return False
    period_end = row.get('current_period_end')
    if period_end is None:
        return True
    if isinstance(period_end, date):
        return period_end >= date.today()
    return True


def fetch_authorized_payment(auth_payment_id, access_token):
    response = requests.get(
        f'https://api.mercadopago.com/authorized_payments/{auth_payment_id}',
        headers={'Authorization': f'Bearer {access_token}'},
        timeout=30,
    )
    if response.status_code != 200:
        logger.error('Erro ao buscar authorized_payment %s: %s', auth_payment_id, response.text)
        return None
    return response.json()


def _parse_json_field(value):
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return []
    return value or []


def _get_payment_row(cursor, reference_id):
    cursor.execute(
        """
        SELECT reference_id, customer_email, plan_id, plan_name,
               selected_states, selected_areas,
               extra_states, extra_states_price,
               extra_areas, extra_areas_price,
               customer_name
        FROM payments
        WHERE reference_id = %s
        """,
        (reference_id,),
    )
    return cursor.fetchone()


def _ensure_user_id(cursor, reference_id, payment_row):
    email = payment_row[1]
    cursor.execute('SELECT id FROM users WHERE email = %s', (email,))
    user_result = cursor.fetchone()
    if user_result:
        return user_result[0]

    cursor.execute(
        """
        SELECT customer_name, customer_cpf, customer_phone,
               customer_empresa, customer_cnpj, customer_senha_hash
        FROM payments
        WHERE reference_id = %s
        """,
        (reference_id,),
    )
    customer_data = cursor.fetchone()
    if not customer_data:
        logger.error('Dados do cliente não encontrados para referência %s', reference_id)
        return None

    customer_name, customer_cpf, customer_phone, customer_empresa, customer_cnpj, customer_senha_hash = customer_data
    username = email.split('@')[0]
    cursor.execute('SELECT COUNT(*) FROM users WHERE username = %s', (username,))
    count = cursor.fetchone()[0]
    if count > 0:
        username = f'{username}_{count + 1}'

    if not customer_senha_hash:
        temp_password = secrets.token_urlsafe(12)
        customer_senha_hash = bcrypt.hashpw(
            temp_password.encode('utf-8'),
            bcrypt.gensalt(),
        ).decode('utf-8')
        logger.warning('Senha não encontrada no checkout; gerada senha temporária para %s', email)

    cursor.execute(
        """
        INSERT INTO users (
            username, email, password_hash, full_name,
            phone, company_name, cnpj_cpf,
            created_at, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
        RETURNING id
        """,
        (
            username,
            email,
            customer_senha_hash,
            customer_name,
            customer_phone,
            customer_empresa,
            customer_cnpj if customer_cnpj else customer_cpf,
        ),
    )
    user_id = cursor.fetchone()[0]
    logger.info('Usuário criado com ID %s para %s', user_id, email)
    return user_id


def _upsert_subscription(cursor, user_id, payment_row, reference_id, mp_preapproval_id=None, extend_period=True):
    selected_states = _parse_json_field(payment_row[4])
    selected_areas = _parse_json_field(payment_row[5])

    period_end = None
    if extend_period:
        period_end = date.today() + timedelta(days=SUBSCRIPTION_PERIOD_DAYS)

    cursor.execute(
        """
        INSERT INTO subscriptions (
            user_id, plan_id, plan_name, status,
            selected_states, selected_areas,
            payment_reference, mp_preapproval_id,
            current_period_end, billing_type,
            start_date, created_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
        ON CONFLICT (user_id)
        DO UPDATE SET
            plan_id = EXCLUDED.plan_id,
            plan_name = EXCLUDED.plan_name,
            status = EXCLUDED.status,
            selected_states = EXCLUDED.selected_states,
            selected_areas = EXCLUDED.selected_areas,
            payment_reference = EXCLUDED.payment_reference,
            mp_preapproval_id = COALESCE(EXCLUDED.mp_preapproval_id, subscriptions.mp_preapproval_id),
            current_period_end = CASE
                WHEN EXCLUDED.current_period_end IS NOT NULL THEN EXCLUDED.current_period_end
                ELSE subscriptions.current_period_end
            END,
            billing_type = EXCLUDED.billing_type,
            updated_at = NOW()
        """,
        (
            user_id,
            payment_row[2],
            payment_row[3],
            'active',
            json.dumps(selected_states),
            json.dumps(selected_areas),
            reference_id,
            mp_preapproval_id,
            period_end,
            'subscription',
        ),
    )


def _extend_subscription_period(cursor, user_id, payment_id):
    cursor.execute(
        """
        UPDATE subscriptions
        SET current_period_end = GREATEST(
                COALESCE(current_period_end, CURRENT_DATE),
                CURRENT_DATE
            ) + %s * INTERVAL '1 day',
            status = 'active',
            last_payment_id = %s,
            updated_at = NOW()
        WHERE user_id = %s
        """,
        (SUBSCRIPTION_PERIOD_DAYS, str(payment_id), user_id),
    )


def _schedule_confirmation_email(payment_row):
    try:
        import threading

        from src.services.email_service import send_payment_confirmation

        states = _parse_json_field(payment_row[4])
        areas = _parse_json_field(payment_row[5])
        args = (payment_row[1], payment_row[10], payment_row[3], states, areas)
        thread = threading.Thread(target=lambda: send_payment_confirmation(*args), daemon=True)
        thread.start()
        logger.info('Email de confirmação agendado para %s', payment_row[0])
    except Exception as exc:
        logger.error('Erro ao agendar email de confirmação: %s', exc)


def _schedule_cancellation_email(user_email, user_name, plan_name):
    try:
        import threading

        from src.services.email_service import send_subscription_cancellation

        thread = threading.Thread(
            target=lambda: send_subscription_cancellation(user_email, user_name, plan_name),
            daemon=True,
        )
        thread.start()
        logger.info('Email de cancelamento agendado para %s', user_email)
    except Exception as exc:
        logger.error('Erro ao agendar email de cancelamento: %s', exc)


def activate_subscription_from_reference(cursor, reference_id, mp_preapproval_id=None, mp_payment_id=None, send_email=True):
    payment_row = _get_payment_row(cursor, reference_id)
    if not payment_row:
        logger.warning('Pagamento não encontrado para referência %s', reference_id)
        return False

    user_id = _ensure_user_id(cursor, reference_id, payment_row)
    if not user_id:
        return False

    _upsert_subscription(cursor, user_id, payment_row, reference_id, mp_preapproval_id=mp_preapproval_id)

    if mp_preapproval_id:
        cursor.execute(
            """
            UPDATE payments
            SET mp_preapproval_id = %s,
                billing_type = 'subscription',
                status = 'authorized',
                updated_at = NOW()
            WHERE reference_id = %s
            """,
            (mp_preapproval_id, reference_id),
        )

    if mp_payment_id:
        cursor.execute(
            """
            UPDATE payments
            SET payment_id = %s, updated_at = NOW()
            WHERE reference_id = %s
            """,
            (str(mp_payment_id), reference_id),
        )
        cursor.execute(
            """
            UPDATE subscriptions
            SET last_payment_id = %s, updated_at = NOW()
            WHERE user_id = %s
            """,
            (str(mp_payment_id), user_id),
        )

    if send_email:
        _schedule_confirmation_email(payment_row)

    logger.info('Assinatura ativada para user_id=%s ref=%s', user_id, reference_id)
    return True


def renew_subscription_charge(cursor, preapproval_id, payment_id, external_reference=None):
    cursor.execute(
        """
        SELECT user_id, last_payment_id, payment_reference
        FROM subscriptions
        WHERE mp_preapproval_id = %s
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (preapproval_id,),
    )
    sub = cursor.fetchone()

    reference_id = external_reference
    if sub:
        if sub[1] and str(sub[1]) == str(payment_id):
            logger.info('Cobrança %s já processada (idempotente)', payment_id)
            return True
        _extend_subscription_period(cursor, sub[0], payment_id)
        reference_id = reference_id or sub[2]
        logger.info('Assinatura renovada user_id=%s até novo período', sub[0])
    elif reference_id:
        activate_subscription_from_reference(
            cursor,
            str(reference_id),
            mp_preapproval_id=preapproval_id,
            mp_payment_id=payment_id,
            send_email=True,
        )
        return True
    else:
        logger.warning('Assinatura não encontrada para preapproval %s', preapproval_id)
        return False

    if reference_id:
        cursor.execute(
            """
            UPDATE payments
            SET payment_id = %s, status = 'approved', updated_at = NOW()
            WHERE reference_id = %s
            """,
            (str(payment_id), reference_id),
        )
    return True


def link_preapproval(cursor, reference_id, preapproval_id, status):
    cursor.execute(
        """
        UPDATE payments
        SET mp_preapproval_id = %s,
            billing_type = 'subscription',
            status = %s,
            updated_at = NOW()
        WHERE reference_id = %s
        """,
        (preapproval_id, status, reference_id),
    )

    cursor.execute(
        """
        UPDATE subscriptions
        SET mp_preapproval_id = %s, updated_at = NOW()
        WHERE payment_reference = %s
        """,
        (preapproval_id, reference_id),
    )


def update_subscription_by_preapproval_status(cursor, preapproval):
    preapproval_id = preapproval.get('id')
    status = (preapproval.get('status') or '').lower()
    reference_id = preapproval.get('external_reference')
    if reference_id is not None:
        reference_id = str(reference_id)

    if reference_id:
        link_preapproval(cursor, reference_id, preapproval_id, status)

    if status == 'authorized':
        logger.info('Preapproval autorizado: %s (aguardando cobrança)', preapproval_id)
        return True

    if status in ('cancelled', 'canceled', 'paused'):
        new_status = 'cancelled' if status in ('cancelled', 'canceled') else 'past_due'
        cursor.execute(
            """
            UPDATE subscriptions
            SET status = %s, updated_at = NOW()
            WHERE mp_preapproval_id = %s
            """,
            (new_status, preapproval_id),
        )
        logger.info('Assinatura %s marcada como %s', preapproval_id, new_status)
        return True

    return True


def process_authorized_payment_notification(cursor, authorized_payment):
    nested = authorized_payment.get('payment') or {}
    payment_status = nested.get('status')
    payment_id = nested.get('id')
    preapproval_id = authorized_payment.get('preapproval_id')
    external_reference = authorized_payment.get('external_reference')

    if external_reference is not None:
        external_reference = str(external_reference)

    if payment_status != 'approved' or not payment_id:
        logger.info(
            'authorized_payment ignorado: status=%s payment_id=%s',
            payment_status,
            payment_id,
        )
        return True

    return renew_subscription_charge(
        cursor,
        preapproval_id,
        payment_id,
        external_reference=external_reference,
    )


def cancel_mp_preapproval(preapproval_id, sdk=None, access_token=None):
    """Cancela assinatura recorrente no Mercado Pago."""
    if not preapproval_id:
        return True, None

    for status_value in ('cancelled', 'canceled'):
        if sdk:
            try:
                response = sdk.preapproval().update(preapproval_id, {'status': status_value})
                http_status = response.get('status')
                if http_status in (200, 201):
                    logger.info('Preapproval %s cancelado no MP (status=%s)', preapproval_id, status_value)
                    return True, response.get('response')
            except Exception as exc:
                logger.warning('SDK falhou ao cancelar preapproval %s: %s', preapproval_id, exc)

        if access_token:
            response = requests.put(
                f'https://api.mercadopago.com/preapproval/{preapproval_id}',
                headers={
                    'Authorization': f'Bearer {access_token}',
                    'Content-Type': 'application/json',
                },
                json={'status': status_value},
                timeout=30,
            )
            if response.status_code in (200, 201):
                logger.info('Preapproval %s cancelado via REST (status=%s)', preapproval_id, status_value)
                return True, response.json()
            logger.warning(
                'REST falhou ao cancelar preapproval %s status=%s body=%s',
                preapproval_id,
                status_value,
                response.text,
            )

    return False, None


def cancel_subscription_for_user(cursor, user_id, sdk=None, access_token=None):
    """
    Cancela assinatura ativa do usuário no Mercado Pago e no banco.
    Retorna dict com success, message e subscription_id.
    """
    cursor.execute(
        """
        SELECT id, mp_preapproval_id, payment_reference, plan_name, status
        FROM subscriptions
        WHERE user_id = %s AND status = 'active'
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (user_id,),
    )
    row = cursor.fetchone()
    if not row:
        return {'success': False, 'error': 'Nenhuma assinatura ativa encontrada.'}

    sub_id, mp_preapproval_id, payment_reference, plan_name, _status = row

    if sdk and mp_preapproval_id:
        mp_ok, _mp_response = cancel_mp_preapproval(
            mp_preapproval_id,
            sdk=sdk,
            access_token=access_token,
        )
        if not mp_ok:
            return {
                'success': False,
                'error': (
                    'Não foi possível cancelar a cobrança no Mercado Pago. '
                    'Tente novamente ou entre em contato com o suporte.'
                ),
            }

    cursor.execute(
        """
        UPDATE subscriptions
        SET status = 'cancelled',
            current_period_end = LEAST(COALESCE(current_period_end, CURRENT_DATE), CURRENT_DATE),
            updated_at = NOW()
        WHERE id = %s
        """,
        (sub_id,),
    )

    if payment_reference:
        cursor.execute(
            """
            UPDATE payments
            SET status = 'cancelled', updated_at = NOW()
            WHERE reference_id = %s
            """,
            (payment_reference,),
        )

    cursor.execute(
        'SELECT email, full_name, username FROM users WHERE id = %s',
        (user_id,),
    )
    user_row = cursor.fetchone()
    if user_row:
        user_email, full_name, username = user_row
        display_name = full_name or username or 'Cliente'
        _schedule_cancellation_email(user_email, display_name, plan_name)

    logger.info(
        'Assinatura cancelada user_id=%s sub_id=%s preapproval=%s',
        user_id,
        sub_id,
        mp_preapproval_id,
    )
    return {
        'success': True,
        'message': f'Assinatura do plano {plan_name or "Seglicit"} cancelada com sucesso.',
        'subscription_id': sub_id,
    }
