#!/usr/bin/env python3
"""
Re-scrape / enriquecer licitações incompletas (valor, itens).

Uso básico (reparo em lote):
  python reparar_licitacoes_incompletas.py
  python reparar_licitacoes_incompletas.py --days 14 --limit 10

Testar uma licitação específica (sem alterar o banco):
  python reparar_licitacoes_incompletas.py --pncp-id "00394452000103-1-012349/2026" --dry-run -v
  python reparar_licitacoes_incompletas.py --id 12345 --dry-run -v

Aplicar correção no banco:
  python reparar_licitacoes_incompletas.py --pncp-id "00394452000103-1-012349/2026" --apply
  python reparar_licitacoes_incompletas.py --limit 10 --apply

Métodos:
  --method auto   (default) API PNCP primeiro, Playwright se ainda incompleto
  --method api    somente API (rápido, recomendado)
  --method playwright  somente browser
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))
load_dotenv(SCRIPT_DIR / ".env")

from src.utils.pncp_api_enrichment import (
    DATES_ONLY_DELAY,
    apply_publicacao_index,
    build_publicacao_index,
    enrich_edital_dates_bulk,
    enrich_edital_from_pncp_api,
    parse_pncp_id,
)
from src.utils.tender_enrichment import enrich_edital_scrape_data
from src.utils.tender_dates import coerce_date
from pncp_scraper_items_only import PNCPScraperItemsOnly


def db_connect():
    return psycopg2.connect(
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT', 5432),
        database=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        client_encoding='utf8',
    )


TENDER_SELECT = """
        SELECT id, pncp_id, detail_url, title,
               valor_total_estimado, items_count, items_json, estimated_value,
               publication_date, proposal_start_date, proposal_end_date,
               objeto, prazo, downloaded_files_json, downloads_count
        FROM tenders
"""


def fetch_tender_by_pncp_id(pncp_id: str):
    conn = db_connect()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cursor.execute(
        f"{TENDER_SELECT} WHERE pncp_id = %s",
        (pncp_id,),
    )
    row = cursor.fetchone()
    conn.close()
    return row


def fetch_tender_by_id(tender_id: int):
    conn = db_connect()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cursor.execute(
        f"{TENDER_SELECT} WHERE id = %s",
        (tender_id,),
    )
    row = cursor.fetchone()
    conn.close()
    return row


def fetch_incomplete_tenders(days: int, limit: int, dates_only: bool = False):
    conn = db_connect()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    if dates_only:
        cursor.execute(
            f"""
            {TENDER_SELECT}
            WHERE created_at >= NOW() - (%s || ' days')::interval
              AND pncp_id IS NOT NULL
              AND proposal_end_date IS NULL
              AND items_count IS NOT NULL
              AND items_count > 0
            ORDER BY created_at DESC
            LIMIT %s
            """,
            (days, limit),
        )
    else:
        cursor.execute(
            f"""
            {TENDER_SELECT}
            WHERE created_at >= NOW() - (%s || ' days')::interval
              AND pncp_id IS NOT NULL
              AND (
                valor_total_estimado IS NULL
                OR items_json IS NULL
                OR items_count IS NULL
                OR items_count = 0
                OR proposal_end_date IS NULL
                OR objeto IS NULL
                OR objeto = ''
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


def _parse_json_field(raw):
    if not raw:
        return None
    if isinstance(raw, (list, dict)):
        return raw
    try:
        return json.loads(raw)
    except Exception:
        return None


def row_to_edital(row: dict) -> dict:
    items = _parse_json_field(row.get('items_json')) or []
    files = _parse_json_field(row.get('downloaded_files_json')) or []
    edital = {
        'pncp_id': row.get('pncp_id'),
        'detail_url': row.get('detail_url'),
        'valor_total_estimado': row.get('valor_total_estimado'),
        'estimated_value': row.get('estimated_value'),
        'publication_date': row.get('publication_date'),
        'proposal_start_date': row.get('proposal_start_date'),
        'proposal_end_date': row.get('proposal_end_date'),
        'objeto': row.get('objeto'),
        'prazo': row.get('prazo'),
    }
    if items:
        edital['items'] = items
        edital['items_count'] = row.get('items_count') or len(items)
    if files:
        edital['downloaded_files'] = files
        edital['downloads_count'] = row.get('downloads_count') or len(files)
    return edital


def is_incomplete(edital: dict) -> bool:
    """Falha grave: sem valor e sem itens (reparo não trouxe dados úteis)."""
    items = edital.get('items') or []
    valor = edital.get('valor_total_estimado')
    return (valor is None or valor == 0) and len(items) == 0


def is_partial(edital: dict) -> bool:
    return (
        not edital.get('proposal_end_date')
        or not edital.get('objeto')
        or not edital.get('downloaded_files')
    )


def print_row_state(label: str, row: dict, verbose: bool):
    items_count = row.get('items_count') or 0
    valor = row.get('valor_total_estimado') or row.get('estimated_value')
    print(f"\n{'='*60}")
    print(f"📋 {label}")
    print(f"   ID: {row.get('id')}  |  PNCP: {row.get('pncp_id')}")
    print(f"   Título: {(row.get('title') or '')[:70]}")
    print(f"   DB → valor: {valor or 'NULL'}  |  items_count: {items_count}")
    if verbose and row.get('items_json'):
        try:
            items = json.loads(row['items_json']) if isinstance(row['items_json'], str) else row['items_json']
            print(f"   DB → items_json: {len(items)} item(ns) no banco")
        except Exception:
            pass
    parsed = parse_pncp_id(row.get('pncp_id') or '')
    print(f"   API parseável: {'sim' if parsed else 'NÃO — pncp_id inválido'}")


def print_enriched_result(edital: dict, verbose: bool):
    items = edital.get('items') or []
    files = edital.get('downloaded_files') or []
    valor = edital.get('valor_total_estimado')
    api_status = edital.get('_pncp_api_status', 'n/a')
    print(f"   Resultado → valor: {valor or 'NULL'}  |  itens: {len(items)}  |  arquivos: {len(files)}  |  API: {api_status}")
    print(f"   Datas → início: {edital.get('proposal_start_date') or 'NULL'}  |  fim: {edital.get('proposal_end_date') or 'NULL'}")
    print(f"   Prazo: {edital.get('prazo') or 'NULL'}")
    if verbose and items:
        for i, item in enumerate(items[:5], 1):
            desc = (item.get('descricao') or '')[:55]
            vt = item.get('valor_total')
            print(f"      {i}. {desc}…  total={vt}")
        if len(items) > 5:
            print(f"      … +{len(items)-5} itens")


def repair_via_api(row: dict, verbose: bool, dates_only: bool = False, pub_index: Optional[dict] = None) -> dict:
    edital = row_to_edital(row)
    if dates_only:
        if pub_index is not None:
            enrich_edital_dates_bulk(edital, pub_index)
        else:
            enrich_edital_from_pncp_api(edital, dates_only=True)
    else:
        enrich_edital_from_pncp_api(edital, force=False)
        edital = enrich_edital_scrape_data(edital)
    if verbose:
        print_enriched_result(edital, verbose=True)
        if is_partial(edital):
            print('   ⚠️  Parcial — datas/objeto/arquivos podem ficar pendentes (API consulta lenta)')
    return edital


def is_dates_repair_failed(edital: dict) -> bool:
    return not edital.get('proposal_end_date')


async def repair_via_playwright(scraper: PNCPScraperItemsOnly, row: dict, verbose: bool) -> dict:
    url = scraper.resolve_detail_url(row.get('detail_url')) or row.get('detail_url')
    if not url:
        raise ValueError('detail_url ausente')
    if verbose:
        print(f"   🌐 Playwright → {url}")
    await scraper.page.goto(url, timeout=45000, wait_until='domcontentloaded')
    await scraper.wait_for_detail_page()
    detail_data = await scraper.scrape_detail_page(0)
    if scraper.is_scrape_incomplete(detail_data):
        await scraper.page.wait_for_timeout(3000)
        await scraper.wait_for_detail_page(timeout_ms=10000)
        detail_data = await scraper.scrape_detail_page(0)
    edital = {
        'pncp_id': row['pncp_id'],
        **detail_data,
        'detail_url': scraper.page.url,
    }
    edital = enrich_edital_scrape_data(edital)
    if verbose:
        print_enriched_result(edital, verbose=True)
    return edital


async def repair_one(row: dict, scraper: Optional[PNCPScraperItemsOnly], method: str, verbose: bool, dates_only: bool = False, pub_index: Optional[dict] = None) -> dict:
    edital = None
    if method in ('auto', 'api'):
        edital = repair_via_api(row, verbose, dates_only=dates_only, pub_index=pub_index)
    if not dates_only and (method == 'playwright' or (method == 'auto' and is_incomplete(edital or {}))):
        if scraper is None:
            raise RuntimeError('Playwright necessário mas browser não iniciado')
        if verbose and method == 'auto':
            print('   ⚠️  API insuficiente — tentando Playwright…')
        edital = await repair_via_playwright(scraper, row, verbose)
    return edital or {}


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
            objeto = COALESCE(NULLIF(%s, ''), objeto),
            modality = COALESCE(NULLIF(%s, ''), modality),
            detailed_description = COALESCE(NULLIF(%s, ''), detailed_description),
            prazo = COALESCE(NULLIF(%s, ''), prazo),
            proposal_start_date = COALESCE(%s, proposal_start_date),
            proposal_end_date = COALESCE(%s, proposal_end_date),
            detail_url = COALESCE(NULLIF(%s, ''), detail_url),
            data_source = %s
        WHERE pncp_id = %s
        RETURNING id, valor_total_estimado, items_count, proposal_end_date, downloads_count
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
            edital.get('objeto') or '',
            edital.get('modality') or '',
            edital.get('detailed_description') or '',
            edital.get('prazo') or '',
            proposal_start,
            proposal_end,
            edital.get('detail_url') or '',
            f'PNCP_REPAIR_{datetime.now().strftime("%Y%m%d")}',
            edital.get('pncp_id'),
        ),
    )
    updated = cursor.fetchone()
    conn.commit()
    return updated


async def run_repairs(rows: List[dict], method: str, apply: bool, verbose: bool, dates_only: bool = False):
    if not rows:
        print("✅ Nenhuma licitação para processar.")
        return

    mode = 'datas-only' if dates_only else 'completo'
    print(f"📋 {len(rows)} licitação(ões) | método={method} | modo={mode} | apply={'sim' if apply else 'DRY-RUN'}")

    conn = None
    cursor = None
    if apply:
        conn = db_connect()
        cursor = conn.cursor()

    headless = os.getenv('SCRAPER_HEADLESS', 'true').lower() in ('1', 'true', 'yes')
    need_browser = method == 'playwright' or method == 'auto'

    repaired = 0
    failed = 0

    pub_index = None
    if dates_only and method == 'api':
        unique_dates = {coerce_date(r.get('publication_date')) for r in rows}
        unique_dates.discard(None)
        if unique_dates:
            print(f"📅 Modo bulk: buscando publicações PNCP para {len(unique_dates)} dia(s)...")
            try:
                pub_index = build_publicacao_index(unique_dates)
                print(f"✅ Índice bulk: {len(pub_index)} contrato(s) — consulta individual só para faltantes")
            except Exception as exc:
                print(f"⚠️  Índice bulk falhou ({exc}) — usando consulta individual")

    async def process_batch(scraper=None):
        nonlocal repaired, failed
        for row in rows:
            print_row_state('Licitação', row, verbose)
            try:
                edital = await repair_one(row, scraper, method, verbose, dates_only=dates_only, pub_index=pub_index)
                if dates_only:
                    if is_dates_repair_failed(edital):
                        print(f"   ❌ Datas ainda não obtidas (PNCP consulta indisponível ou rate limit)")
                        failed += 1
                        continue
                elif is_incomplete(edital):
                    print(f"   ❌ Ainda incompleta após reparo (sem valor e sem itens)")
                    failed += 1
                    continue
                if apply:
                    result = upsert_repaired(cursor, conn, edital)
                    msg = f"   ✅ Banco atualizado → id={result[0]} valor={result[1]} items={result[2]} fim={result[3]} arquivos={result[4]}"
                    if not dates_only and is_partial(edital):
                        msg += " (parcial: datas/objeto/arquivos pendentes)"
                    print(msg)
                else:
                    note = ""
                    if dates_only:
                        note = ""
                    elif is_partial(edital):
                        note = " (parcial)"
                    print(f"   ✅ [DRY-RUN] Corrigível{note} — use --apply para gravar no banco")
                repaired += 1
                if method == 'api' and not dates_only:
                    time.sleep(float(os.getenv('PNCP_REPAIR_DELAY', '0.25')))
                elif method == 'api' and dates_only and pub_index is None:
                    time.sleep(DATES_ONLY_DELAY)
            except Exception as exc:
                if conn:
                    conn.rollback()
                print(f"   ❌ Erro: {exc}")
                failed += 1

    if need_browser and method != 'api':
        async with PNCPScraperItemsOnly(headless=headless) as scraper:
            await process_batch(scraper)
    else:
        await process_batch(None)

    if conn:
        conn.close()

    print(f"\n🏁 Concluído: {repaired} ok, {failed} falha(s)")


def resolve_rows(args) -> List[dict]:
    rows = []
    if args.pncp_id:
        for pid in args.pncp_id:
            row = fetch_tender_by_pncp_id(pid)
            if row:
                rows.append(dict(row))
            else:
                print(f"⚠️  pncp_id não encontrado no banco: {pid}")
                rows.append({'pncp_id': pid, 'detail_url': None, 'title': pid, 'id': None})
    if args.id:
        for tid in args.id:
            row = fetch_tender_by_id(tid)
            if row:
                rows.append(dict(row))
            else:
                print(f"⚠️  id não encontrado: {tid}")
    if not rows and not args.pncp_id and not args.id:
        rows = [dict(r) for r in fetch_incomplete_tenders(args.days, args.limit, dates_only=args.dates_only)]
    elif args.dates_only and rows:
        rows = [r for r in rows if not r.get('proposal_end_date')]
    return rows


def main():
    parser = argparse.ArgumentParser(description='Reparar / testar licitações incompletas')
    parser.add_argument('--days', type=int, default=14, help='Janela em dias para lote (default: 14)')
    parser.add_argument('--limit', type=int, default=30, help='Máximo no lote (default: 30)')
    parser.add_argument('--pncp-id', action='append', dest='pncp_id', help='PNCP ID específico (pode repetir)')
    parser.add_argument('--id', action='append', dest='id', type=int, help='ID interno tenders (pode repetir)')
    parser.add_argument('--method', choices=['auto', 'api', 'playwright'], default='auto')
    parser.add_argument('--dates-only', action='store_true', help='Só preenche datas/prazo/objeto (sem itens/arquivos)')
    parser.add_argument('--apply', action='store_true', help='Gravar no banco (sem isso = dry-run)')
    parser.add_argument('--dry-run', action='store_true', help='Alias explícito de não aplicar')
    parser.add_argument('-v', '--verbose', action='store_true')
    args = parser.parse_args()

    apply = args.apply and not args.dry_run
    if not args.pncp_id and not args.id and not args.apply and not args.dry_run:
        # lote: mantém comportamento de aplicar por default
        apply = True

    rows = resolve_rows(args)
    asyncio.run(run_repairs(rows, args.method, apply, args.verbose, dates_only=args.dates_only))


if __name__ == '__main__':
    main()
