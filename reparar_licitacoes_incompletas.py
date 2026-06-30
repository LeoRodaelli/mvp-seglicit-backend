#!/usr/bin/env python3
"""
Re-scrape licitações incompletas (sem valor, itens ou arquivos) dos últimos N dias.

Uso:
  python reparar_licitacoes_incompletas.py
  python reparar_licitacoes_incompletas.py --days 14 --limit 20
"""

import argparse
import asyncio
import json
import os
import sys
from datetime import datetime
from pathlib import Path

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))
load_dotenv(SCRIPT_DIR / ".env")

from src.utils.tender_enrichment import enrich_edital_scrape_data
from src.utils.tender_dates import coerce_date
from pncp_scraper_items_only import PNCPScraperItemsOnly


def fetch_incomplete_tenders(days: int, limit: int):
    conn = psycopg2.connect(
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT', 5432),
        database=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        client_encoding='utf8',
    )
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cursor.execute(
        """
        SELECT id, pncp_id, detail_url, title
        FROM tenders
        WHERE created_at >= NOW() - (%s || ' days')::interval
          AND detail_url IS NOT NULL
          AND detail_url != ''
          AND (
            valor_total_estimado IS NULL
            OR items_json IS NULL
            OR items_count IS NULL
            OR items_count = 0
            OR downloaded_files_json IS NULL
            OR downloads_count IS NULL
            OR downloads_count = 0
          )
        ORDER BY created_at DESC
        LIMIT %s
        """,
        (days, limit),
    )
    rows = cursor.fetchall()
    conn.close()
    return rows


def upsert_repaired(cursor, conn, edital):
    edital = enrich_edital_scrape_data(edital)
    items = edital.get('items') or []
    files = edital.get('downloaded_files') or []
    items_json = json.dumps(items, ensure_ascii=False) if items else None
    files_json = json.dumps(files, ensure_ascii=False) if files else None
    valor = edital.get('valor_total_estimado')
    estimated = edital.get('estimated_value') or valor
    proposal_start = coerce_date(edital.get('proposal_start_date'))
    proposal_end = coerce_date(edital.get('proposal_end_date'))

    cursor.execute(
        """
        UPDATE tenders SET
            valor_total_estimado = COALESCE(%s, valor_total_estimado),
            estimated_value = COALESCE(%s, estimated_value),
            items_json = CASE WHEN %s > 0 THEN %s ELSE items_json END,
            items_count = GREATEST(COALESCE(items_count, 0), %s),
            downloaded_files_json = CASE WHEN %s > 0 THEN %s ELSE downloaded_files_json END,
            downloads_count = GREATEST(COALESCE(downloads_count, 0), %s),
            detailed_description = COALESCE(NULLIF(%s, ''), detailed_description),
            prazo = COALESCE(NULLIF(%s, ''), prazo),
            proposal_start_date = COALESCE(%s, proposal_start_date),
            proposal_end_date = COALESCE(%s, proposal_end_date),
            data_source = %s
        WHERE pncp_id = %s
        """,
        (
            valor,
            estimated,
            len(items),
            items_json,
            len(items),
            len(files),
            files_json,
            len(files),
            edital.get('detailed_description') or '',
            edital.get('prazo') or '',
            proposal_start,
            proposal_end,
            f'PNCP_REPAIR_{datetime.now().strftime("%Y%m%d")}',
            edital.get('pncp_id'),
        ),
    )
    conn.commit()


async def repair_tender(scraper: PNCPScraperItemsOnly, row):
    url = scraper.resolve_detail_url(row['detail_url']) or row['detail_url']
    print(f"🔧 Reparando: {row['title'][:60]}...")
    await scraper.page.goto(url, timeout=45000, wait_until='domcontentloaded')
    await scraper.wait_for_detail_page()

    detail_data = await scraper.scrape_detail_page(0)

    if scraper.is_scrape_incomplete(detail_data):
        await scraper.page.wait_for_timeout(5000)
        await scraper.wait_for_detail_page(timeout_ms=15000)
        detail_data = await scraper.scrape_detail_page(0)

    return {
        'pncp_id': row['pncp_id'],
        **detail_data,
        'detail_url': scraper.page.url,
    }


async def main(days: int, limit: int):
    rows = fetch_incomplete_tenders(days, limit)
    if not rows:
        print("✅ Nenhuma licitação incompleta encontrada.")
        return

    print(f"📋 {len(rows)} licitação(ões) incompleta(s) para reparar")

    conn = psycopg2.connect(
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT', 5432),
        database=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        client_encoding='utf8',
    )
    cursor = conn.cursor()

    headless = os.getenv('SCRAPER_HEADLESS', 'true').lower() in ('1', 'true', 'yes')

    async with PNCPScraperItemsOnly(headless=headless) as scraper:
        repaired = 0
        for row in rows:
            try:
                edital = await repair_tender(scraper, row)
                upsert_repaired(cursor, conn, edital)
                repaired += 1
                print(f"  ✅ {row['pncp_id']}")
            except Exception as exc:
                conn.rollback()
                print(f"  ❌ {row['pncp_id']}: {exc}")
        print(f"🏁 Reparadas: {repaired}/{len(rows)}")

    conn.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Reparar licitações incompletas no banco')
    parser.add_argument('--days', type=int, default=14, help='Janela em dias (default: 14)')
    parser.add_argument('--limit', type=int, default=30, help='Máximo de licitações (default: 30)')
    args = parser.parse_args()
    asyncio.run(main(args.days, args.limit))
