# -*- coding: utf-8 -*-
"""
Busca de licitações reutilizável, respeitando o plano do usuário.

Extraída da lógica do endpoint /api/zaia/buscar para poder ser chamada
tanto pelo agente Zaia quanto pelo chat interno (Claude), sem duplicar
a filtragem por plano.
"""
import logging

import psycopg2.extras

from src.utils.insurance_guarantee import requires_seguro_garantia, seguro_garantia_sql_clause

logger = logging.getLogger(__name__)


def search_tenders_for_user(current_user, q='', estados_param='', areas_param='',
                             data_inicio='', data_fim='', limit=10,
                             seguro_garantia_only=False):
    """
    Busca licitações publicadas respeitando o plano do usuário (mesma regra
    usada em /api/zaia/buscar: `_resolve_zaia_agent_search_filters`).

    Retorna dict:
      { 'error': str|None, 'items': [...], 'estados': [...],
        'plan_name': str|None, 'scope_note': str|None }
    """
    from src.routes.zaia_api import _resolve_zaia_agent_search_filters, get_db_connection

    subscription = current_user.get('subscription')
    filters = _resolve_zaia_agent_search_filters(subscription, estados_param, areas_param)
    if filters.get('error'):
        return {'error': filters['error'], 'items': []}

    estados_lista = filters['estados']
    keywords_areas = filters['keywords_areas']
    scope_note = filters.get('scope_note')
    plan_name = filters.get('plan_name')

    conn = get_db_connection()
    if not conn:
        return {'error': 'Erro ao conectar ao banco de dados. Tente novamente.', 'items': []}

    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        conditions = ["status = 'Publicado'"]
        params = []

        if q:
            conditions.append(
                "(title ILIKE %s OR objeto ILIKE %s OR organization_name ILIKE %s OR description ILIKE %s)"
            )
            params.extend([f'%{q}%'] * 4)

        if estados_lista:
            placeholders = ','.join(['%s'] * len(estados_lista))
            conditions.append(f"state_code IN ({placeholders})")
            params.extend(estados_lista)

        if keywords_areas:
            kw_conditions = []
            for kw in keywords_areas:
                kw_conditions.append("(title ILIKE %s OR objeto ILIKE %s OR description ILIKE %s)")
                params.extend([f'%{kw}%'] * 3)
            conditions.append(f"({' OR '.join(kw_conditions)})")

        if data_inicio:
            conditions.append("publication_date >= %s")
            params.append(data_inicio)
        if data_fim:
            conditions.append("publication_date <= %s")
            params.append(data_fim)

        if seguro_garantia_only:
            conditions.append(seguro_garantia_sql_clause())

        where_clause = ' AND '.join(conditions)
        query = f"""
            SELECT id, title, objeto, description, detailed_description,
                   organization_name, municipality_name, state_code,
                   COALESCE(valor_total_estimado, estimated_value) AS valor,
                   publication_date, detail_url, source_url
            FROM tenders
            WHERE {where_clause}
            ORDER BY publication_date DESC
            LIMIT %s
        """
        params.append(limit)

        cur.execute(query, params)
        rows = cur.fetchall()
        cur.close()
        conn.close()

        items = []
        for row in rows:
            items.append({
                'id': row['id'],
                'titulo': row['title'] or 'Sem título',
                'objeto': row['objeto'] or '',
                'orgao': row['organization_name'] or 'Não informado',
                'municipio': row['municipality_name'] or '',
                'estado': row['state_code'] or '',
                'valor': float(row['valor']) if row['valor'] else None,
                'data_publicacao': str(row['publication_date'])[:10] if row['publication_date'] else None,
                'link': row['detail_url'] or row['source_url'] or None,
                'exige_seguro_garantia': requires_seguro_garantia(
                    row.get('objeto'), row.get('description'), row.get('detailed_description')
                ),
            })

        return {
            'error': None,
            'items': items,
            'estados': estados_lista,
            'plan_name': plan_name,
            'scope_note': scope_note,
        }
    except Exception as e:
        logger.error('Erro na busca de licitações (chat): %s', e)
        try:
            conn.close()
        except Exception:
            pass
        return {'error': f'Erro ao realizar a busca: {e}', 'items': []}
