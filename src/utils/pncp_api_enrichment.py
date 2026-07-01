"""Enriquece licitações via API pública do PNCP (itens + valor estimado)."""

from __future__ import annotations

import logging
import re
import time
from typing import Any, Dict, List, Optional, Tuple

import requests

logger = logging.getLogger(__name__)

PNCP_API_BASE = 'https://pncp.gov.br/api/pncp/v1'
PNCP_ID_RE = re.compile(r'^(\d{14})-\d+-(\d+)/(\d{4})$')

_session = requests.Session()
_session.headers.update({
    'User-Agent': 'Seglicit-Bot/1.0',
    'Accept': 'application/json',
})


def parse_pncp_id(pncp_id: str) -> Optional[Tuple[str, int, int]]:
    """
    Converte pncp_id (ex: 00394452000103-1-012349/2026) em (cnpj, ano, sequencial).
    """
    if not pncp_id:
        return None
    raw = pncp_id.strip()
    match = PNCP_ID_RE.match(raw)
    if not match:
        return None
    cnpj, sequencial_raw, ano_raw = match.groups()
    return cnpj, int(ano_raw), int(sequencial_raw)


def _get_json(url: str, params: Optional[dict] = None) -> Any:
    response = _session.get(url, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def fetch_contract(cnpj: str, ano: int, sequencial: int) -> Optional[Dict]:
    url = f'{PNCP_API_BASE}/orgaos/{cnpj}/compras/{ano}/{sequencial}'
    try:
        return _get_json(url)
    except Exception as exc:
        logger.warning('PNCP contrato %s/%s/%s: %s', cnpj, ano, sequencial, exc)
        return None


def fetch_items(cnpj: str, ano: int, sequencial: int) -> List[Dict]:
    """Busca todos os itens paginados da contratação."""
    url = f'{PNCP_API_BASE}/orgaos/{cnpj}/compras/{ano}/{sequencial}/itens'
    all_items: List[Dict] = []
    page = 1

    while page <= 20:
        try:
            payload = _get_json(url, {'pagina': page, 'tamanhoPagina': 50})
        except Exception as exc:
            logger.warning('PNCP itens página %s: %s', page, exc)
            break

        batch = payload
        if isinstance(payload, dict):
            batch = payload.get('data') or payload.get('itens') or []

        if not batch:
            break

        all_items.extend(batch)
        if len(batch) < 50:
            break
        page += 1
        time.sleep(0.15)

    return all_items


def map_api_item(raw: Dict, index: int) -> Dict:
    valor_total = raw.get('valorTotal')
    if raw.get('orcamentoSigiloso') and not valor_total:
        valor_total = 'Sigiloso'

    return {
        'numero': str(raw.get('numeroItem') or index + 1),
        'descricao': (raw.get('descricao') or f'Item {index + 1}')[:500],
        'quantidade': raw.get('quantidade'),
        'unidade': raw.get('unidadeMedida') or '',
        'valor_unitario': raw.get('valorUnitarioEstimado'),
        'valor_total': valor_total,
    }


def sum_item_values(items: List[Dict]) -> Optional[float]:
    total = 0.0
    found = False
    for item in items:
        try:
            value = item.get('valor_total')
            if value is None or value == 'Sigiloso':
                continue
            total += float(value)
            found = True
        except (TypeError, ValueError):
            continue
    return total if found and total > 0 else None


def enrich_edital_from_pncp_api(edital: Dict, *, force: bool = False) -> Dict:
    """
    Preenche items + valor_total_estimado via API PNCP quando ausentes no scrape.
    Retorna o mesmo dict (mutado) e adiciona chaves de diagnóstico _pncp_api_*.
    """
    if not edital:
        return edital

    has_items = bool(edital.get('items'))
    has_valor = edital.get('valor_total_estimado') not in (None, '', 0)
    if not force and has_items and has_valor:
        edital['_pncp_api_status'] = 'skipped_already_complete'
        return edital

    pncp_id = edital.get('pncp_id') or ''
    parsed = parse_pncp_id(pncp_id)
    if not parsed:
        edital['_pncp_api_status'] = 'invalid_pncp_id'
        return edital

    cnpj, ano, sequencial = parsed

    try:
        contract = fetch_contract(cnpj, ano, sequencial)
        raw_items = fetch_items(cnpj, ano, sequencial)
    except Exception as exc:
        edital['_pncp_api_status'] = f'error:{exc}'
        return edital

    mapped = [map_api_item(raw, i) for i, raw in enumerate(raw_items)]

    if mapped and (force or not has_items):
        edital['items'] = mapped
        edital['items_count'] = len(mapped)
        edital['items_tab_found'] = True

    valor = None
    if contract:
        valor = contract.get('valorTotalEstimado')
        if valor in (0, 0.0):
            valor = None

    if valor is None:
        valor = sum_item_values(mapped)

    if valor is not None and (force or not has_valor):
        edital['valor_total_estimado'] = float(valor)
        if not edital.get('estimated_value'):
            edital['estimated_value'] = float(valor)

    edital['_pncp_api_status'] = 'ok'
    edital['_pncp_api_items'] = len(mapped)
    edital['_pncp_api_valor'] = valor
    return edital
