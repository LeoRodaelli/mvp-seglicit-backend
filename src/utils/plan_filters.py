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


def _area_matches_allowed(allowed_area, requested_area):
    """Verifica se uma área solicitada está coberta pelo plano."""
    allowed = (allowed_area or "").strip().lower()
    requested = (requested_area or "").strip().lower()
    if not allowed or not requested:
        return False
    if allowed == requested or allowed in requested or requested in allowed:
        return True
    for area_key in KEYWORDS_POR_AREA:
        key_lower = area_key.lower()
        if requested in key_lower or key_lower in requested:
            if allowed == area_key or allowed in key_lower or key_lower in allowed:
                return True
    return False


def resolve_plan_search_scope(subscription, estados_param="", areas_param=""):
    """
    Cruza filtros solicitados pelo agente com os limites do plano ativo.

    Retorna dict com:
    - error: mensagem para o usuário ou None
    - estados / areas: listas efetivas para a busca
    - plan_name, allowed_states, allowed_areas, note
    """
    if not subscription:
        return {
            "error": (
                "Você não possui um plano ativo. "
                "Assine um plano em seglicit.com.br para buscar licitações pelo assistente."
            ),
            "estados": [],
            "areas": [],
            "plan_name": None,
            "allowed_states": [],
            "allowed_areas": [],
            "note": None,
        }

    plan_name = subscription.get("plan_name") or "seu plano"
    allowed_states = [
        str(s).strip().upper()
        for s in (subscription.get("selected_states") or [])
        if s and str(s).strip()
    ]
    allowed_areas = [
        str(a).strip()
        for a in (subscription.get("selected_areas") or [])
        if a and str(a).strip()
    ]

    if not allowed_states:
        return {
            "error": (
                f"Seu plano {plan_name} não possui estados configurados. "
                "Entre em contato com o suporte em seglicit@gmail.com."
            ),
            "estados": [],
            "areas": [],
            "plan_name": plan_name,
            "allowed_states": [],
            "allowed_areas": allowed_areas,
            "note": None,
        }

    if not allowed_areas:
        return {
            "error": (
                f"Seu plano {plan_name} não possui áreas de atuação configuradas. "
                "Entre em contato com o suporte em seglicit@gmail.com."
            ),
            "estados": [],
            "areas": [],
            "plan_name": plan_name,
            "allowed_states": allowed_states,
            "allowed_areas": [],
            "note": None,
        }

    requested_states = [
        e.strip().upper()
        for e in (estados_param or "").split(",")
        if e and e.strip()
    ]
    rejected_states = []
    estados_efetivos = []

    if requested_states:
        for state in requested_states:
            if state in allowed_states:
                if state not in estados_efetivos:
                    estados_efetivos.append(state)
            else:
                rejected_states.append(state)

        if rejected_states and not estados_efetivos:
            return {
                "error": (
                    f"Seu plano {plan_name} inclui apenas os estados "
                    f"{', '.join(allowed_states)}. "
                    f"Não é possível buscar licitações em {', '.join(rejected_states)}. "
                    f"Informe um estado do seu plano para continuar."
                ),
                "estados": [],
                "areas": [],
                "plan_name": plan_name,
                "allowed_states": allowed_states,
                "allowed_areas": allowed_areas,
                "note": None,
            }
    else:
        estados_efetivos = list(allowed_states)

    areas_efetivas = list(allowed_areas)
    rejected_areas = []
    if areas_param and str(areas_param).strip():
        requested_areas = [
            a.strip()
            for a in str(areas_param).split("|")
            if a and a.strip()
        ]
        if requested_areas:
            matched = []
            for req in requested_areas:
                found = False
                for allowed in allowed_areas:
                    if _area_matches_allowed(allowed, req):
                        if allowed not in matched:
                            matched.append(allowed)
                        found = True
                        break
                if not found:
                    rejected_areas.append(req)

            if rejected_areas and not matched:
                return {
                    "error": (
                        f"Seu plano {plan_name} inclui apenas as áreas: "
                        f"{', '.join(allowed_areas)}. "
                        f"A área solicitada ({', '.join(rejected_areas)}) não está no seu plano."
                    ),
                    "estados": estados_efetivos,
                    "areas": [],
                    "plan_name": plan_name,
                    "allowed_states": allowed_states,
                    "allowed_areas": allowed_areas,
                    "note": None,
                }
            if matched:
                areas_efetivas = matched

    note_parts = []
    if rejected_states and estados_efetivos:
        note_parts.append(
            f"O(s) estado(s) {', '.join(rejected_states)} não fazem parte do seu plano; "
            f"a busca foi limitada a {', '.join(estados_efetivos)}."
        )
    if rejected_areas and areas_efetivas:
        note_parts.append(
            f"Área(s) fora do plano ({', '.join(rejected_areas)}) foram ignoradas."
        )

    return {
        "error": None,
        "estados": estados_efetivos,
        "areas": areas_efetivas,
        "plan_name": plan_name,
        "allowed_states": allowed_states,
        "allowed_areas": allowed_areas,
        "note": " ".join(note_parts) if note_parts else None,
    }


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
