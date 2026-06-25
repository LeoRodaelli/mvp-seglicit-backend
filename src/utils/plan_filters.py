"""Filtros de licitações baseados no plano (estados + áreas)."""
import json

from src.routes.zaia_api import KEYWORDS_POR_AREA


def parse_json_list(value):
    if not value:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, list) else []
        except Exception:
            return []
    return []


def get_keywords_for_areas(areas):
    """
    Retorna keywords na ordem estável do frontend:
    percorre cada área na ordem do plano e preserva a ordem do dicionário.
    """
    keywords = []
    seen = set()
    for area in areas or []:
        for kw in KEYWORDS_POR_AREA.get(area, []):
            normalized = kw.lower()
            if normalized not in seen:
                seen.add(normalized)
                keywords.append(normalized)
    return keywords


def build_plan_filter(state_codes=None, areas=None):
    """
    Monta cláusulas SQL e parâmetros para filtrar tenders pelo plano.

    Regra:
    - estados: AND (state_code IN (...))
    - áreas: AND (área1 OR área2 OR ...) — licitação precisa bater em ao menos uma área
    """
    clauses = []
    params = []

    if state_codes:
        normalized_states = [s.strip().upper() for s in state_codes if s and str(s).strip()]
        if normalized_states:
            placeholders = ",".join(["%s"] * len(normalized_states))
            clauses.append(f"state_code IN ({placeholders})")
            params.extend(normalized_states)

    if areas:
        area_clauses = []
        for area in areas:
            keywords = KEYWORDS_POR_AREA.get(area, [])
            if not keywords:
                fallback = area.lower()
                area_clauses.append(
                    "(title ILIKE %s OR objeto ILIKE %s OR description ILIKE %s)"
                )
                params.extend([f"%{fallback}%"] * 3)
                continue

            keyword_conditions = []
            for kw in keywords:
                keyword_conditions.append(
                    "(title ILIKE %s OR objeto ILIKE %s OR description ILIKE %s)"
                )
                params.extend([f"%{kw.lower()}%"] * 3)
            area_clauses.append("(" + " OR ".join(keyword_conditions) + ")")

        if area_clauses:
            clauses.append("(" + " OR ".join(area_clauses) + ")")

    if not clauses:
        return "", []

    return " AND ".join(clauses), params
