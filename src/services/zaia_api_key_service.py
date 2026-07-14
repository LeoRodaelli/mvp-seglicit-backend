# -*- coding: utf-8 -*-
"""Geração e garantia de API Keys Zaia por usuário."""
import secrets
from datetime import datetime

ACTIVE_SUBSCRIPTION_SQL = """
    SELECT id, plan_name, selected_states, selected_areas
    FROM subscriptions
    WHERE user_id = %s
      AND status = 'active'
      AND (current_period_end IS NULL OR current_period_end >= CURRENT_DATE)
    ORDER BY created_at DESC
    LIMIT 1
"""


def get_active_subscription_row(cursor, user_id):
    cursor.execute(ACTIVE_SUBSCRIPTION_SQL, (user_id,))
    return cursor.fetchone()


def ensure_user_zaia_api_key(cursor, user_id):
    """
    Retorna a API Key existente ou cria uma nova para o usuário ativo.
    Retorna None se o usuário não existir ou estiver inativo.
    """
    cursor.execute(
        """
        SELECT zaia_api_key
        FROM users
        WHERE id = %s AND is_active = true
        """,
        (user_id,),
    )
    row = cursor.fetchone()
    if not row:
        return None

    existing_key = row["zaia_api_key"] if isinstance(row, dict) else row[0]
    if existing_key:
        return existing_key

    new_key = "zaia_sk_" + secrets.token_urlsafe(32)
    cursor.execute(
        """
        UPDATE users
        SET zaia_api_key = %s, updated_at = %s
        WHERE id = %s AND is_active = true
        """,
        (new_key, datetime.now(), user_id),
    )
    return new_key


def regenerate_user_zaia_api_key(cursor, user_id):
    """Força a rotação da API Key (uso administrativo / endpoint com senha)."""
    new_key = "zaia_sk_" + secrets.token_urlsafe(32)
    cursor.execute(
        """
        UPDATE users
        SET zaia_api_key = %s, updated_at = %s
        WHERE id = %s AND is_active = true
        """,
        (new_key, datetime.now(), user_id),
    )
    return new_key
