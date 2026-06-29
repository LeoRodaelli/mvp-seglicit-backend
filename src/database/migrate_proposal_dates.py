#!/usr/bin/env python3
"""
Migration: colunas proposal_start_date / proposal_end_date + backfill + expiração.

Uso (Railway/local):
  python src/database/migrate_proposal_dates.py
"""

import os
import sys
from pathlib import Path

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / '.env')

from src.utils.tender_dates import parse_proposal_dates_from_text


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
        ALTER TABLE tenders
        ADD COLUMN IF NOT EXISTS proposal_start_date DATE;
    """)
    cursor.execute("""
        ALTER TABLE tenders
        ADD COLUMN IF NOT EXISTS proposal_end_date DATE;
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_tenders_proposal_end_date
        ON tenders (proposal_end_date);
    """)


def backfill_from_text(cursor):
    cursor.execute("""
        SELECT id, detailed_description, description, prazo
        FROM tenders
        WHERE proposal_end_date IS NULL
          AND (
            detailed_description IS NOT NULL
            OR description IS NOT NULL
            OR prazo IS NOT NULL
          )
    """)
    rows = cursor.fetchall()
    updated = 0
    for row in rows:
        blob = ' '.join(
            filter(None, [row['detailed_description'], row['description'], row['prazo']])
        )
        start, end = parse_proposal_dates_from_text(blob)
        if not start and not end:
            continue
        cursor.execute(
            """
            UPDATE tenders
            SET proposal_start_date = COALESCE(%s, proposal_start_date),
                proposal_end_date = COALESCE(%s, proposal_end_date)
            WHERE id = %s
            """,
            (start, end, row['id']),
        )
        updated += 1
    return updated


def mark_expired(cursor):
    cursor.execute("""
        UPDATE tenders
        SET status = 'Expirada'
        WHERE proposal_end_date IS NOT NULL
          AND proposal_end_date < CURRENT_DATE
          AND LOWER(COALESCE(status, '')) NOT IN ('expirada', 'cancelado', 'encerrada', 'encerrado', 'finalizada')
    """)
    return cursor.rowcount


def main():
    conn = get_conn()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        print('📦 Criando colunas proposal_start_date / proposal_end_date...')
        ensure_columns(cursor)
        conn.commit()

        print('🔄 Backfill a partir de detailed_description...')
        backfilled = backfill_from_text(cursor)
        conn.commit()
        print(f'   ✅ {backfilled} registro(s) atualizado(s)')

        print('⏳ Marcando licitações encerradas...')
        expired = mark_expired(cursor)
        conn.commit()
        print(f'   ✅ {expired} licitação(ões) marcada(s) como Expirada')

        print('✅ Migration concluída.')
    finally:
        cursor.close()
        conn.close()


if __name__ == '__main__':
    main()
