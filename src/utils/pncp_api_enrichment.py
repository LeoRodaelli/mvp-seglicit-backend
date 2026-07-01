"""Enriquece licitações via API pública do PNCP (itens, valor, datas, arquivos)."""

from __future__ import annotations

import logging
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
    response = _session.get(url, params=params, timeout=timeout)
    response.raise_for_status()
    return response.json()


def fetch_contract(cnpj: str, ano: int, sequencial: int) -> Optional[Dict]:
    """Detalhes da contratação (datas, objeto, valor) — API consulta."""
    url = f'{CONSULTA_API_BASE}/orgaos/{cnpj}/compras/{ano}/{sequencial}'
    for attempt in range(2):
        try:
            data = _get_json(url, timeout=60 if attempt else 45)
            if isinstance(data, dict) and (
                data.get('dataAberturaProposta') is not None
                or data.get('objetoCompra')
                or data.get('valorTotalEstimado')
            ):
                return data
            if isinstance(data, dict) and not data.get('message'):
                return data
        except Exception as exc:
            logger.warning('PNCP consulta contrato %s/%s/%s (tentativa %s): %s', cnpj, ano, sequencial, attempt + 1, exc)
            time.sleep(0.5)
    return None


def fetch_contract_via_publicacao(pncp_id: str, publication_date=None) -> Optional[Dict]:
    """Fallback: busca contratação na listagem de publicações por data + pncp_id."""
    pub = coerce_date(publication_date)
    if not pub:
        return None

    url = f'{CONSULTA_API_BASE}/contratacoes/publicacao'
    for day_offset in (0, -1, 1):
        target = pub + timedelta(days=day_offset)
        params = {
            'dataInicial': target.isoformat(),
            'dataFinal': target.isoformat(),
            'pagina': 1,
            'tamanhoPagina': 100,
        }
        try:
            payload = _get_json(url, params, timeout=60)
            rows = payload.get('data') if isinstance(payload, dict) else payload
            if not rows:
                continue
            for row in rows:
                ctrl = (row.get('numeroControlePNCP') or row.get('numeroControlePncp') or '').strip()
                if ctrl == pncp_id.strip():
                    return row
        except Exception as exc:
            logger.warning('PNCP publicacao %s (%s): %s', pncp_id, target, exc)
            time.sleep(0.3)
    return None


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


def enrich_edital_from_pncp_api(edital: Dict, *, force: bool = False) -> Dict:
    """
    Preenche via API PNCP: itens, valor, datas de proposta, prazo, objeto e arquivos.
    """
    if not edital:
        return edital

    if not _needs_enrichment(edital, force):
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
        if not contract or not contract.get('dataEncerramentoProposta'):
            pub_contract = fetch_contract_via_publicacao(pncp_id, edital.get('publication_date'))
            if pub_contract:
                contract = {**(contract or {}), **pub_contract}
        raw_items = fetch_items(cnpj, ano, sequencial)
        raw_files = fetch_arquivos(cnpj, ano, sequencial)
    except Exception as exc:
        edital['_pncp_api_status'] = f'error:{exc}'
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

    edital['_pncp_api_status'] = 'ok'
    edital['_pncp_api_items'] = len(mapped_items)
    edital['_pncp_api_files'] = len(mapped_files)
    edital['_pncp_api_valor'] = valor
    edital['_pncp_api_has_dates'] = bool(edital.get('proposal_end_date'))
    return edital
