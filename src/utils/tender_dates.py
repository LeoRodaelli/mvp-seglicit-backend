"""Datas de proposta (início/fim) e helpers de expiração de licitações."""

import re
from datetime import date, datetime
from typing import Optional, Tuple

# Fallback quando não há data fim no PNCP (dias após publicação)
LEGACY_OPEN_DAYS = 60

_START_PATTERNS = [
    r'Data de in[ií]cio de recebimento de propostas:\s*(\d{2}/\d{2}/\d{4})',
    r'Data in[ií]cio de recebimento de propostas:\s*(\d{2}/\d{2}/\d{4})',
    r'In[ií]cio de recebimento de propostas:\s*(\d{2}/\d{2}/\d{4})',
]

_END_PATTERNS = [
    r'Data fim de recebimento de propostas:\s*(\d{2}/\d{2}/\d{4})',
    r'Data final de recebimento de propostas:\s*(\d{2}/\d{2}/\d{4})',
    r'Fim de recebimento de propostas:\s*(\d{2}/\d{2}/\d{4})',
    r'Encerramento de propostas:\s*(\d{2}/\d{2}/\d{4})',
]

_CLOSED_STATUSES = frozenset({'expirada', 'cancelado', 'encerrada', 'encerrado', 'finalizada'})


def _parse_br_date(raw: str) -> Optional[date]:
    if not raw:
        return None
    try:
        return datetime.strptime(raw.strip(), '%d/%m/%Y').date()
    except ValueError:
        return None


def _first_match(text: str, patterns) -> Optional[date]:
    if not text:
        return None
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            parsed = _parse_br_date(match.group(1))
            if parsed:
                return parsed
    return None


def parse_proposal_dates_from_text(text: str) -> Tuple[Optional[date], Optional[date]]:
    """Extrai datas de início/fim de recebimento de propostas do texto PNCP."""
    if not text:
        return None, None
    start = _first_match(text, _START_PATTERNS)
    end = _first_match(text, _END_PATTERNS)
    return start, end


def coerce_date(value) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        if len(raw) >= 10 and raw[4] == '-':
            try:
                return date.fromisoformat(raw[:10])
            except ValueError:
                pass
        return _parse_br_date(raw)
    return None


def tender_is_open(
    proposal_end_date,
    publication_date=None,
    status=None,
    *,
    legacy_open_days: int = LEGACY_OPEN_DAYS,
) -> bool:
    if status and str(status).strip().lower() in _CLOSED_STATUSES:
        return False

    end = coerce_date(proposal_end_date)
    if end is not None:
        return end >= date.today()

    pub = coerce_date(publication_date)
    if pub is not None:
        return (date.today() - pub).days <= legacy_open_days

    return True


def days_until_proposal_close(proposal_end_date) -> Optional[int]:
    end = coerce_date(proposal_end_date)
    if end is None:
        return None
    return (end - date.today()).days


def proposal_close_label(proposal_end_date, status=None) -> str:
    if status and str(status).strip().lower() in _CLOSED_STATUSES:
        return 'Encerrada'

    days = days_until_proposal_close(proposal_end_date)
    if days is None:
        return ''

    if days < 0:
        return 'Encerrada'
    if days == 0:
        return 'Encerra hoje'
    if days == 1:
        return 'Encerra amanhã'
    if days <= 7:
        return f'Encerra em {days} dias'
    return f'Propostas até {coerce_date(proposal_end_date).strftime("%d/%m/%Y")}'


def open_tender_sql_clause(table_alias: str = '') -> str:
    """SQL: licitações abertas (com fallback legacy por publication_date)."""
    prefix = f'{table_alias}.' if table_alias else ''
    return f"""
        LOWER(COALESCE({prefix}status, '')) NOT IN ('expirada', 'cancelado', 'encerrada', 'encerrado', 'finalizada')
        AND (
            ({prefix}proposal_end_date IS NOT NULL AND {prefix}proposal_end_date >= CURRENT_DATE)
            OR (
                {prefix}proposal_end_date IS NULL
                AND {prefix}publication_date IS NOT NULL
                AND {prefix}publication_date >= CURRENT_DATE - INTERVAL '{LEGACY_OPEN_DAYS} days'
            )
            OR (
                {prefix}proposal_end_date IS NULL
                AND {prefix}publication_date IS NULL
            )
        )
    """
