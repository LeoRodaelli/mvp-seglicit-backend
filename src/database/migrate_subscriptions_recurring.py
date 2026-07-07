#!/usr/bin/env python3
"""
Migration: colunas para assinatura recorrente Mercado Pago (preapproval).

Uso (Railway/local):
  python src/database/migrate_subscriptions_recurring.py
"""

import os
import sys
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / '.env')


def get_conn():
    return psycopg2.connect(
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT', 5432),
        database=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        client_encoding='utf8',
    )


def ensure_columns(cursor):
    cursor.execute("""
        ALTER TABLE subscriptions
        ADD COLUMN IF NOT EXISTS mp_preapproval_id VARCHAR(64);
    """)
    cursor.execute("""
        ALTER TABLE subscriptions
        ADD COLUMN IF NOT EXISTS current_period_end DATE;
    """)
    cursor.execute("""
        ALTER TABLE subscriptions
        ADD COLUMN IF NOT EXISTS last_payment_id VARCHAR(64);
    """)
    cursor.execute("""
        ALTER TABLE subscriptions
        ADD COLUMN IF NOT EXISTS billing_type VARCHAR(32) DEFAULT 'one_time';
    """)
    cursor.execute("""
        ALTER TABLE payments
        ADD COLUMN IF NOT EXISTS mp_preapproval_id VARCHAR(64);
    """)
    cursor.execute("""
        ALTER TABLE payments
        ADD COLUMN IF NOT EXISTS billing_type VARCHAR(32) DEFAULT 'one_time';
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_subscriptions_mp_preapproval_id
        ON subscriptions (mp_preapproval_id);
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_subscriptions_period_end
        ON subscriptions (current_period_end);
    """)


def main():
    conn = get_conn()
    cursor = conn.cursor()
    try:
        print('📦 Criando colunas de assinatura recorrente...')
        ensure_columns(cursor)
        conn.commit()
        print('✅ Migration concluída.')
    finally:
        cursor.close()
        conn.close()


if __name__ == '__main__':
    main()
