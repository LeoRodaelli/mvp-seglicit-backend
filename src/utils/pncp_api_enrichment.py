"""Enriquece licitações via API pública do PNCP (itens, valor, datas, arquivos)."""

from __future__ import annotations

import logging
import os
import re
import time
from typing import Any, Dict, List, Optional, Tuple

import requests

from datetime import date, timedelta

from src.utils.tender_dates import coerce_date, format_proposal_prazo, parse_api_datetime

logger = logging.getLogger(__name__)

PNCP_API_BASE = 'https://pncp.gov.br/api/pncp/v1'
CONSULTA_API_BASE = 'https://pncp.gov.br/api/consulta/v1'
PNCP_ID_RE = re.compile(r'^(\d{14})-\d+-(\d+)/(\d{4})$')
CONSULTA_TIMEOUT = int(os.getenv('PNCP_CONSULTA_TIMEOUT', '18'))
CONSULTA_RETRIES = max(1, int(os.getenv('PNCP_CONSULTA_RETRIES', '3')))
SKIP_CONSULTA = os.getenv('PNCP_SKIP_CONSULTA', '').lower() in ('1', 'true', 'yes')
REPAIR_DELAY = float(os.getenv('PNCP_REPAIR_DELAY', '0.25'))
DATES_ONLY_DELAY = float(os.getenv('PNCP_DATES_DELAY', '3.0'))

_session = requests.Session()
_session.headers.update({
    'User-Agent': 'Seglicit-Bot/1.0',
    'Accept': 'application/json',
})


def parse_pncp_id(pncp_id: str) -> Optional[Tuple[str, int, int]]:
    """Converte pncp_id (ex: 00394452000103-1-012349/2026) em (cnpj, ano, sequencial)."""
    if not pncp_id:
        return None
    raw = pncp_id.strip()
    match = PNCP_ID_RE.match(raw)
    if not match:
        return None
    cnpj, sequencial_raw, ano_raw = match.groups()
    return cnpj, int(ano_raw), int(sequencial_raw)


def _get_json(url: str, params: Optional[dict] = None, timeout: int = 45) -> Any:
    last_exc = None
    for attempt in range(CONSULTA_RETRIES):
        try:
            response = _session.get(url, params=params, timeout=timeout)
            if response.status_code in (429, 503):
                wait = min(30, 2 ** attempt * 2)
                logger.debug('PNCP %s — aguardando %ss (tentativa %s)', response.status_code, wait, attempt + 1)
                time.sleep(wait)
                continue
            response.raise_for_status()
            return response.json()
        except requests.HTTPError as exc:
            last_exc = exc
            status = exc.response.status_code if exc.response is not None else None
            if status in (429, 503) and attempt + 1 < CONSULTA_RETRIES:
                time.sleep(min(30, 2 ** attempt * 2))
                continue
            raise
        except Exception as exc:
            last_exc = exc
            if attempt + 1 < CONSULTA_RETRIES:
                time.sleep(0.5)
                continue
            raise
    if last_exc:
        raise last_exc
    raise RuntimeError('PNCP request failed without response')


def fetch_contract(cnpj: str, ano: int, sequencial: int) -> Optional[Dict]:
    """Detalhes da contratação (datas, objeto, valor) — API consulta (pode estar lenta)."""
    if SKIP_CONSULTA:
        return None

    url = f'{CONSULTA_API_BASE}/orgaos/{cnpj}/compras/{ano}/{sequencial}'
    last_exc = None
    for attempt in range(CONSULTA_RETRIES):
        try:
            data = _get_json(url, timeout=CONSULTA_TIMEOUT)
            if isinstance(data, dict) and (
                data.get('dataAberturaProposta') is not None
                or data.get('objetoCompra')
                or data.get('valorTotalEstimado')
            ):
                return data
            if isinstance(data, dict) and not data.get('message'):
                return data
        except Exception as exc:
            last_exc = exc
            if attempt + 1 < CONSULTA_RETRIES:
                time.sleep(0.4)
    if last_exc:
        logger.info(
            'PNCP consulta indisponível %s/%s/%s (%s) — itens/arquivos seguem normalmente',
            cnpj, ano, sequencial, last_exc,
        )
    return None


def fetch_contract_via_publicacao(pncp_id: str, publication_date=None) -> Optional[Dict]:
    """Fallback: busca contratação na listagem de publicações por data + pncp_id."""
    pub = coerce_date(publication_date)
    if not pub:
        return None

    url = f'{CONSULTA_API_BASE}/contratacoes/publicacao'
    pub_timeout = min(CONSULTA_TIMEOUT, 12)
    consecutive_failures = 0
    for day_offset in (0, -1, 1, -2, 2):
        target = pub + timedelta(days=day_offset)
        params = {
            'dataInicial': target.isoformat(),
            'dataFinal': target.isoformat(),
            'pagina': 1,
            'tamanhoPagina': 100,
        }
        try:
            payload = _get_json(url, params, timeout=pub_timeout)
            consecutive_failures = 0
            rows = payload.get('data') if isinstance(payload, dict) else payload
            if not rows:
                continue
            for row in rows:
                ctrl = (row.get('numeroControlePNCP') or row.get('numeroControlePncp') or '').strip()
                if ctrl == pncp_id.strip():
                    return row
        except Exception as exc:
            consecutive_failures += 1
            logger.debug('PNCP publicacao %s (%s): %s', pncp_id, target, exc)
            if '429' in str(exc) or '503' in str(exc):
                time.sleep(min(30, 2 ** consecutive_failures * 2))
            if consecutive_failures >= 3:
                logger.info(
                    'PNCP publicacao abortada para %s após %s falhas consecutivas',
                    pncp_id, consecutive_failures,
                )
                break
            time.sleep(0.15)
    return None


def _pncp_id_from_row(row: Dict) -> str:
    return (row.get('numeroControlePNCP') or row.get('numeroControlePncp') or '').strip()


def fetch_publicacao_page(target: date, page: int = 1, page_size: int = 100) -> List[Dict]:
    """Uma página da listagem de publicações do dia (até 100 contratos com datas)."""
    url = f'{CONSULTA_API_BASE}/contratacoes/publicacao'
    params = {
        'dataInicial': target.isoformat(),
        'dataFinal': target.isoformat(),
        'pagina': page,
        'tamanhoPagina': page_size,
    }
    payload = _get_json(url, params, timeout=min(CONSULTA_TIMEOUT, 25))
    rows = payload.get('data') if isinstance(payload, dict) else payload
    return rows or []


def fetch_all_publicacao_for_date(target: date) -> List[Dict]:
    """Todas as páginas de publicação de um dia."""
    all_rows: List[Dict] = []
    for page in range(1, 51):
        try:
            batch = fetch_publicacao_page(target, page=page)
        except Exception as exc:
            logger.warning('PNCP publicacao bulk %s pág %s: %s', target, page, exc)
            break
        if not batch:
            break
        all_rows.extend(batch)
        if len(batch) < 100:
            break
        time.sleep(0.35)
    return all_rows


def build_publicacao_index(
    publication_dates,
    *,
    extra_day_offsets: Tuple[int, ...] = (-1, 1),
) -> Dict[str, Dict]:
    """
    Mapa pncp_id → contratação via API publicacao (1 chamada/página por dia).
    Muito mais eficiente que consulta individual por edital.
    """
    index: Dict[str, Dict] = {}
    fetched_days: set = set()

    for raw in publication_dates:
        pub = coerce_date(raw)
        if not pub:
            continue
        days_to_try = [pub]
        for offset in extra_day_offsets:
            days_to_try.append(pub + timedelta(days=offset))

        for target in days_to_try:
            if target in fetched_days:
                continue
            fetched_days.add(target)
            rows = fetch_all_publicacao_for_date(target)
            for row in rows:
                pid = _pncp_id_from_row(row)
                if pid and pid not in index:
                    index[pid] = row
            if rows:
                logger.info('PNCP publicacao bulk %s → %s contrato(s)', target, len(rows))
            time.sleep(0.6)

    return index


def apply_publicacao_index(edital: Dict, index: Dict[str, Dict]) -> bool:
    """Preenche datas/objeto a partir do índice bulk. Retorna True se obteve data fim."""
    pncp_id = (edital.get('pncp_id') or '').strip()
    row = index.get(pncp_id)
    if not row:
        return False
    _apply_contract_fields(edital, row, force=False)
    return bool(edital.get('proposal_end_date'))


def enrich_edital_dates_bulk(edital: Dict, index: Dict[str, Dict]) -> Dict:
    """Tenta índice bulk primeiro; consulta individual só se necessário."""
    if apply_publicacao_index(edital, index):
        edital['_pncp_api_status'] = 'ok_bulk'
        edital['_pncp_api_enriched'] = True
        edital['_pncp_api_has_dates'] = True
        return edital
    return enrich_edital_from_pncp_api(edital, dates_only=True)


def _needs_contract_fields(edital: Dict, force: bool) -> bool:
    if force:
        return True
    if not edital.get('proposal_end_date'):
        return True
    if not edital.get('objeto'):
        return True
    if edital.get('valor_total_estimado') in (None, '', 0):
        return True
    return False


def fetch_items(cnpj: str, ano: int, sequencial: int) -> List[Dict]:
    """Itens da contratação — API pncp/v1."""
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
        time.sleep(0.12)

    return all_items


def fetch_arquivos(cnpj: str, ano: int, sequencial: int) -> List[Dict]:
    """Metadados de arquivos/documentos — API pncp/v1."""
    url = f'{PNCP_API_BASE}/orgaos/{cnpj}/compras/{ano}/{sequencial}/arquivos'
    try:
        payload = _get_json(url)
        if isinstance(payload, list):
            return payload
        if isinstance(payload, dict):
            return payload.get('data') or payload.get('arquivos') or []
    except Exception as exc:
        logger.warning('PNCP arquivos %s/%s/%s: %s', cnpj, ano, sequencial, exc)
    return []


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


def map_api_arquivo(raw: Dict, cnpj: str, ano: int, sequencial: int) -> Dict:
    seq_doc = raw.get('sequencialDocumento') or 1
    title = (raw.get('titulo') or raw.get('tipoDocumentoNome') or 'documento').strip()
    tipo = (raw.get('tipoDocumentoDescricao') or raw.get('tipoDocumentoNome') or 'Documento').strip()
    safe = re.sub(r'[^\w\s.\-()áàâãéêíóôõúçÁÀÂÃÉÊÍÓÔÕÚÇ]', '', title)[:80].strip() or f'documento_{seq_doc}'
    if not safe.lower().endswith('.pdf'):
        safe = f'{safe}.pdf'
    url = raw.get('url') or f'{PNCP_API_BASE}/orgaos/{cnpj}/compras/{ano}/{sequencial}/arquivos/{seq_doc}'
    return {
        'filename': safe,
        'url': url,
        'title': title,
        'tipo': tipo,
        'source': 'pncp_api',
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


def _needs_enrichment(edital: Dict, force: bool) -> bool:
    if force:
        return True
    if not edital.get('items'):
        return True
    if edital.get('valor_total_estimado') in (None, '', 0):
        return True
    if not edital.get('proposal_end_date'):
        return True
    if not edital.get('objeto'):
        return True
    if not edital.get('downloaded_files'):
        return True
    return False


def _apply_contract_fields(edital: Dict, contract: Dict, force: bool) -> None:
    if not contract:
        return

    start = parse_api_datetime(contract.get('dataAberturaProposta'))
    end = parse_api_datetime(contract.get('dataEncerramentoProposta'))

    if start and (force or not edital.get('proposal_start_date')):
        edital['proposal_start_date'] = start.isoformat()
    if end and (force or not edital.get('proposal_end_date')):
        edital['proposal_end_date'] = end.isoformat()

    if force or not edital.get('prazo'):
        prazo = format_proposal_prazo(
            edital.get('proposal_start_date'),
            edital.get('proposal_end_date'),
        )
        if prazo:
            edital['prazo'] = prazo

    objeto = contract.get('objetoCompra') or contract.get('objeto')
    if objeto and (force or not edital.get('objeto')):
        edital['objeto'] = str(objeto)[:2000]

    modalidade = contract.get('modalidadeNome') or contract.get('modalidade')
    if modalidade and (force or not edital.get('modality')):
        edital['modality'] = modalidade

    org = contract.get('orgaoEntidade') or {}
    unit = contract.get('unidadeOrgao') or {}
    if org.get('razaoSocial') and (force or not edital.get('organization_name')):
        edital['organization_name'] = org['razaoSocial']
    if unit.get('municipioNome') and (force or not edital.get('municipality_name')):
        edital['municipality_name'] = unit['municipioNome']
    if unit.get('ufSigla') and (force or not edital.get('state_code')):
        edital['state_code'] = unit['ufSigla']


def enrich_edital_from_pncp_api(edital: Dict, *, force: bool = False, dates_only: bool = False) -> Dict:
    """
    Preenche via API PNCP: itens, valor, datas de proposta, prazo, objeto e arquivos.
    Com dates_only=True, busca só datas/objeto (sem itens/arquivos).
    """
    if not edital:
        return edital

    if dates_only:
        if edital.get('proposal_end_date') and not force:
            edital['_pncp_api_status'] = 'skipped_has_dates'
            return edital
    elif not _needs_enrichment(edital, force):
        edital['_pncp_api_status'] = 'skipped_already_complete'
        return edital

    pncp_id = edital.get('pncp_id') or ''
    parsed = parse_pncp_id(pncp_id)
    if not parsed:
        edital['_pncp_api_status'] = 'invalid_pncp_id'
        return edital

    cnpj, ano, sequencial = parsed

    need_items = not dates_only and (force or not edital.get('items'))
    need_files = not dates_only and (force or not edital.get('downloaded_files'))
    need_contract = dates_only or _needs_contract_fields(edital, force)

    try:
        raw_items = fetch_items(cnpj, ano, sequencial) if need_items else []
        raw_files = fetch_arquivos(cnpj, ano, sequencial) if need_files else []

        contract = None
        if need_contract:
            pub_contract = fetch_contract_via_publicacao(pncp_id, edital.get('publication_date'))
            if pub_contract:
                contract = pub_contract
            if not contract or not contract.get('dataEncerramentoProposta'):
                direct = fetch_contract(cnpj, ano, sequencial)
                if direct:
                    contract = {**(contract or {}), **direct}
    except Exception as exc:
        edital['_pncp_api_status'] = f'error:{exc}'
        return edital

    if dates_only:
        _apply_contract_fields(edital, contract or {}, force=False)
        if edital.get('proposal_end_date'):
            edital['_pncp_api_status'] = 'ok'
        else:
            edital['_pncp_api_status'] = 'partial_no_dates'
        edital['_pncp_api_enriched'] = True
        edital['_pncp_api_has_dates'] = bool(edital.get('proposal_end_date'))
        return edital

    mapped_items = [map_api_item(raw, i) for i, raw in enumerate(raw_items)]
    mapped_files = [map_api_arquivo(raw, cnpj, ano, sequencial) for raw in raw_files]

    has_items = bool(edital.get('items'))
    has_valor = edital.get('valor_total_estimado') not in (None, '', 0)
    has_files = bool(edital.get('downloaded_files'))

    if mapped_items and (force or not has_items):
        edital['items'] = mapped_items
        edital['items_count'] = len(mapped_items)
        edital['items_tab_found'] = True

    if mapped_files and (force or not has_files):
        edital['downloaded_files'] = mapped_files
        edital['downloads_count'] = len(mapped_files)
        edital['files_tab_found'] = True

    _apply_contract_fields(edital, contract or {}, force)

    valor = None
    if contract:
        valor = contract.get('valorTotalEstimado')
        if valor in (0, 0.0):
            valor = None
    if valor is None:
        valor = sum_item_values(mapped_items or edital.get('items') or [])

    if valor is not None and (force or not has_valor):
        edital['valor_total_estimado'] = float(valor)
        if not edital.get('estimated_value'):
            edital['estimated_value'] = float(valor)

    if need_contract and not edital.get('proposal_end_date'):
        edital['_pncp_api_status'] = 'partial_no_dates'
    else:
        edital['_pncp_api_status'] = 'ok'
    edital['_pncp_api_items'] = len(mapped_items)
    edital['_pncp_api_files'] = len(mapped_files)
    edital['_pncp_api_valor'] = valor
    edital['_pncp_api_has_dates'] = bool(edital.get('proposal_end_date'))
    edital['_pncp_api_enriched'] = True
    return edital
