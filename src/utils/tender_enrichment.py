"""Helpers para enriquecer licitações com valor derivado de itens."""


def sum_items_total(items):
    if not items:
        return None
    total = 0.0
    found = False
    for item in items:
        try:
            value = item.get('valor_total')
            if value is not None:
                total += float(value)
                found = True
        except (TypeError, ValueError):
            continue
    return total if found and total > 0 else None


def resolve_tender_value(valor_total_estimado, estimated_value, items):
    if valor_total_estimado not in (None, '', 0):
        try:
            return float(valor_total_estimado)
        except (TypeError, ValueError):
            pass
    if estimated_value not in (None, '', 0):
        try:
            return float(estimated_value)
        except (TypeError, ValueError):
            pass
    return sum_items_total(items)


def enrich_edital_scrape_data(edital):
    """Preenche valor ausente a partir da soma dos itens antes de persistir."""
    if not edital:
        return edital
    items = edital.get('items') or []
    if not edital.get('valor_total_estimado') and items:
        total = sum_items_total(items)
        if total:
            edital['valor_total_estimado'] = total
            if not edital.get('estimated_value'):
                edital['estimated_value'] = total
    return edital
