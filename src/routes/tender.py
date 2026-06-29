from flask import Blueprint, request, jsonify, send_file
import psycopg2
import psycopg2.extras
import logging
import json
import os
from datetime import datetime
from dotenv import load_dotenv

from src.utils.plan_filters import build_plan_filter, parse_json_list
from src.utils.tender_enrichment import resolve_tender_value
from src.utils.tender_dates import (
    tender_is_open,
    days_until_proposal_close,
    proposal_close_label,
    open_tender_sql_clause,
)

load_dotenv()

# Configure logging
logger = logging.getLogger(__name__)

tender_bp = Blueprint('tender', __name__)

def get_db_connection():
    """Cria conexão direta com PostgreSQL"""
    try:
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT', 5432),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            client_encoding='utf8'
        )
        return conn
    except Exception as e:
        logger.error(f"Erro de conexão: {e}")
        return None

def format_brazilian_date(date_str):
    """Formata data para padrão brasileiro"""
    if not date_str:
        return ''
    try:
        if isinstance(date_str, str):
            if '-' in date_str and len(date_str) == 10:
                year, month, day = date_str.split('-')
                return f"{day}/{month}/{year}"
        return str(date_str)
    except:
        return str(date_str)

def format_brazilian_currency(value):
    """Formata valor para moeda brasileira"""
    if not value or value == 0:
        return 'Valor não informado'
    try:
        if isinstance(value, str):
            value = float(value.replace(',', '.'))
        formatted = f"R$ {float(value):,.2f}"
        formatted = formatted.replace(',', 'TEMP').replace('.', ',').replace('TEMP', '.')
        return formatted
    except:
        return 'Valor não informado'

def build_tender_dict(row):
    """Converte row do banco em dicionário formatado"""
    items = []
    downloaded_files = []

    try:
        if row['items_json']:
            items = json.loads(row['items_json'])
    except:
        items = []

    try:
        if row['downloaded_files_json']:
            downloaded_files = json.loads(row['downloaded_files_json'])
    except:
        downloaded_files = []

    resolved_value = resolve_tender_value(
        row['valor_total_estimado'],
        row['estimated_value'],
        items,
    )

    proposal_end = row.get('proposal_end_date')
    proposal_start = row.get('proposal_start_date')
    status = row.get('status')
    publication = row.get('publication_date')
    is_open = tender_is_open(proposal_end, publication, status)
    days_left = days_until_proposal_close(proposal_end)

    return {
        'id': row['id'],
        'pncp_id': row['pncp_id'] or '',
        'title': row['title'] or '',
        'description': row['description'] or '',
        'organization_name': row['organization_name'] or '',
        'organization_cnpj': row['organization_cnpj'] or '',
        'municipality_name': row['municipality_name'] or '',
        'municipality_ibge': row['municipality_ibge'] or '',
        'state_code': row['state_code'] or '',
        'publication_date': str(row['publication_date']) if row['publication_date'] else '',
        'publication_date_br': format_brazilian_date(row['publication_date']),
        'status': row['status'] or '',
        'modality': row['modality'] or '',
        'estimated_value': float(row['estimated_value']) if row['estimated_value'] else None,
        'source_url': row['source_url'] or '',
        'detail_url': row['detail_url'] or '',
        'data_source': row['data_source'] or '',
        'created_at': str(row['created_at']) if row['created_at'] else '',
        'pncp_url': row['detail_url'] or row['source_url'] or '',
        'objeto': row['objeto'] or '',
        'prazo': row['prazo'] or '',
        'proposal_start_date': str(proposal_start) if proposal_start else '',
        'proposal_end_date': str(proposal_end) if proposal_end else '',
        'proposal_end_date_br': format_brazilian_date(proposal_end),
        'is_open': is_open,
        'days_until_close': days_left,
        'proposal_close_label': proposal_close_label(proposal_end, status),
        'detailed_description': row['detailed_description'] or '',
        'valor_total_estimado': resolved_value,
        'valor_total_estimado_br': format_brazilian_currency(resolved_value),
        'items_count': row['items_count'] or len(items),
        'downloads_count': row['downloads_count'] or 0,
        'items': items,
        'downloaded_files': downloaded_files,
        'formatted_value': format_brazilian_currency(resolved_value),
        'has_items': len(items) > 0,
        'has_files': len(downloaded_files) > 0
    }


@tender_bp.route('/tenders', methods=['GET'])
def get_tenders():
    """
    GET /api/tenders

    Parâmetros suportados:
      - page, per_page
      - city_name          : filtro por cidade (ILIKE)
      - state_code         : um ou múltiplos estados (ex: state_code=SP&state_code=RJ)
                             OU separados por vírgula (ex: state_code=SP,RJ)
      - keyword            : uma palavra-chave simples (busca em title, description, objeto)
      - keywords           : múltiplas keywords separadas por vírgula (OR entre elas)
                             ex: keywords=computador,notebook,servidor
      - modality           : filtro por modalidade (ILIKE)
      - valor_min          : valor mínimo estimado
      - valor_max          : valor máximo estimado
      - apenas_hoje        : 'true' para filtrar apenas publicações de hoje
      - apenas_abertas     : 'false' para incluir encerradas (padrão: true)
      - incluir_encerradas : alias de apenas_abertas=false
      - date_from           : data inicial do período (YYYY-MM-DD)
      - date_to             : data final do período (YYYY-MM-DD)
      - user_id             : ID do usuário (usado com plan_filter)
      - plan_filter         : 'true' para aplicar estados/áreas da assinatura ativa
    """
    try:
        page = request.args.get('page', 1, type=int)
        per_page = min(request.args.get('per_page', 10, type=int), 100)
        city_name = request.args.get('city_name', '').strip()
        modality = request.args.get('modality', '').strip()
        keyword = request.args.get('keyword', '').strip()
        keywords_param = request.args.get('keywords', '').strip()
        valor_min = request.args.get('valor_min', type=float)
        valor_max = request.args.get('valor_max', type=float)
        apenas_hoje = request.args.get('apenas_hoje', '').lower() == 'true'
        incluir_encerradas = request.args.get('incluir_encerradas', '').lower() == 'true'
        apenas_abertas_param = request.args.get('apenas_abertas', 'true').lower()
        apenas_abertas = not incluir_encerradas and apenas_abertas_param != 'false'
        date_from = request.args.get('date_from', '').strip()   # YYYY-MM-DD
        date_to = request.args.get('date_to', '').strip()       # YYYY-MM-DD
        user_id = request.args.get('user_id', type=int)
        plan_filter = request.args.get('plan_filter', '').lower() == 'true'
        plan_state_codes = None
        plan_areas = None

        # ── Filtro por IDs específicos (para favoritos) ──────────────────────────────
        # Aceita: ids=1&ids=2&ids=3  OU  ids=1,2,3
        raw_ids = request.args.getlist('ids')
        filter_ids = []
        for i in raw_ids:
            for part in i.split(','):
                part = part.strip()
                if part.isdigit():
                    filter_ids.append(int(part))

        # ── Filtro automático pelo plano (assinatura ativa) ────────────────
        if plan_filter and user_id:
            plan_state_codes, plan_areas = _load_subscription_filters(user_id)

        # ── Múltiplos estados ──────────────────────────────────────────────
        # Aceita: state_code=SP&state_code=RJ  OU  state_code=SP,RJ
        raw_states = request.args.getlist('state_code')
        state_codes = []
        if plan_state_codes:
            state_codes = plan_state_codes
        else:
            for s in raw_states:
                for part in s.split(','):
                    part = part.strip().upper()
                    if part:
                        state_codes.append(part)

        # ── Múltiplas keywords ─────────────────────────────────────────────
        # Aceita: keywords=computador,notebook,servidor
        # OU keyword=computador (retrocompatível)
        all_keywords = []
        use_plan_area_filter = bool(plan_areas)
        if not use_plan_area_filter:
            if keywords_param:
                for kw in keywords_param.split(','):
                    kw = kw.strip()
                    if kw:
                        all_keywords.append(kw)
            elif keyword:
                all_keywords = [keyword]

        # Conectar
        conn = get_db_connection()
        if not conn:
            raise Exception("Erro de conexão com banco")

        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        base_query = """
        SELECT id, pncp_id, title, description, organization_name, organization_cnpj,
               municipality_name, municipality_ibge, state_code, publication_date,
               status, modality, estimated_value, source_url, detail_url,
               data_source, created_at, downloaded_files, objeto, items_json,
               downloaded_files_json, prazo, detailed_description, valor_total_estimado,
               items_count, downloads_count, proposal_start_date, proposal_end_date
        FROM tenders
        WHERE 1=1
        """
        params = []

        if apenas_abertas and not filter_ids:
            base_query += f" AND ({open_tender_sql_clause()})"

        # Filtro por IDs específicos (favoritos)
        if filter_ids:
            placeholders = ','.join(['%s'] * len(filter_ids))
            base_query += f" AND id IN ({placeholders})"
            params.extend(filter_ids)

        # Filtro por cidade
        if city_name:
            base_query += " AND municipality_name ILIKE %s"
            params.append(f'%{city_name}%')

        # Filtro por múltiplos estados (IN)
        if state_codes:
            placeholders = ','.join(['%s'] * len(state_codes))
            base_query += f" AND state_code IN ({placeholders})"
            params.extend(state_codes)

        # Filtro por modalidade
        if modality:
            base_query += " AND modality ILIKE %s"
            params.append(f'%{modality}%')

        # Filtro por valor mínimo
        if valor_min is not None:
            base_query += " AND COALESCE(valor_total_estimado, estimated_value) >= %s"
            params.append(valor_min)

        # Filtro por valor máximo
        if valor_max is not None:
            base_query += " AND COALESCE(valor_total_estimado, estimated_value) <= %s"
            params.append(valor_max)

        # Filtro apenas hoje
        if apenas_hoje:
            base_query += " AND DATE(publication_date) = CURRENT_DATE"

        # Filtro por período (data inicial e data final)
        if date_from:
            base_query += " AND DATE(publication_date) >= %s"
            params.append(date_from)
        if date_to:
            base_query += " AND DATE(publication_date) <= %s"
            params.append(date_to)

        # Filtro por áreas do plano (cada área com OR interno; entre áreas também OR)
        if use_plan_area_filter:
            area_clause, area_params = build_plan_filter(None, plan_areas)
            if area_clause:
                base_query += f" AND {area_clause}"
                params.extend(area_params)

        # Filtro por múltiplas keywords (OR entre elas, busca em title + objeto + description)
        elif all_keywords:
            keyword_conditions = []
            for kw in all_keywords:
                keyword_conditions.append(
                    "(title ILIKE %s OR objeto ILIKE %s OR description ILIKE %s)"
                )
                params.extend([f'%{kw}%', f'%{kw}%', f'%{kw}%'])
            base_query += " AND (" + " OR ".join(keyword_conditions) + ")"

        # Ordenar e paginar
        base_query += " ORDER BY publication_date DESC, created_at DESC"
        base_query += f" LIMIT {per_page} OFFSET {(page - 1) * per_page}"

        cursor.execute(base_query, params)
        rows = cursor.fetchall()
        tenders = [build_tender_dict(row) for row in rows]

        # ── Count query (mesma lógica, sem LIMIT/OFFSET) ───────────────────
        count_query = "SELECT COUNT(*) FROM tenders WHERE 1=1"
        count_params = []

        if apenas_abertas and not filter_ids:
            count_query += f" AND ({open_tender_sql_clause()})"

        # Filtro por IDs específicos (favoritos)
        if filter_ids:
            placeholders = ','.join(['%s'] * len(filter_ids))
            count_query += f" AND id IN ({placeholders})"
            count_params.extend(filter_ids)

        if city_name:
            count_query += " AND municipality_name ILIKE %s"
            count_params.append(f'%{city_name}%')

        if state_codes:
            placeholders = ','.join(['%s'] * len(state_codes))
            count_query += f" AND state_code IN ({placeholders})"
            count_params.extend(state_codes)

        if modality:
            count_query += " AND modality ILIKE %s"
            count_params.append(f'%{modality}%')

        if valor_min is not None:
            count_query += " AND COALESCE(valor_total_estimado, estimated_value) >= %s"
            count_params.append(valor_min)

        if valor_max is not None:
            count_query += " AND COALESCE(valor_total_estimado, estimated_value) <= %s"
            count_params.append(valor_max)

        if apenas_hoje:
            count_query += " AND DATE(publication_date) = CURRENT_DATE"

        if date_from:
            count_query += " AND DATE(publication_date) >= %s"
            count_params.append(date_from)
        if date_to:
            count_query += " AND DATE(publication_date) <= %s"
            count_params.append(date_to)

        if use_plan_area_filter:
            area_clause, area_params = build_plan_filter(None, plan_areas)
            if area_clause:
                count_query += f" AND {area_clause}"
                count_params.extend(area_params)
        elif all_keywords:
            keyword_conditions = []
            for kw in all_keywords:
                keyword_conditions.append(
                    "(title ILIKE %s OR objeto ILIKE %s OR description ILIKE %s)"
                )
                count_params.extend([f'%{kw}%', f'%{kw}%', f'%{kw}%'])
            count_query += " AND (" + " OR ".join(keyword_conditions) + ")"

        cursor.execute(count_query, count_params)
        total = cursor.fetchone()['count']

        cursor.close()
        conn.close()

        pages = (total + per_page - 1) // per_page

        return jsonify({
            'success': True,
            'tenders': tenders,
            'pagination': {
                'page': page,
                'pages': pages,
                'per_page': per_page,
                'total': total,
                'has_next': page < pages,
                'has_prev': page > 1
            },
            'filters_applied': {
                'city_name': city_name,
                'state_codes': state_codes,
                'keywords_count': len(all_keywords),
                'modality': modality,
                'valor_min': valor_min,
                'valor_max': valor_max,
                'apenas_hoje': apenas_hoje,
                'apenas_abertas': apenas_abertas and not filter_ids,
            }
        })

    except Exception as e:
        logger.error(f"Error fetching tenders: {e}")
        return jsonify({
            'success': False,
            'error': 'Erro interno do servidor',
            'tenders': [],
            'pagination': {
                'page': 1, 'pages': 0, 'per_page': 10,
                'total': 0, 'has_next': False, 'has_prev': False
            }
        }), 500


@tender_bp.route('/tenders/<int:tender_id>', methods=['GET'])
def get_tender_details(tender_id):
    """Get detalhes completos de uma licitação"""
    try:
        conn = get_db_connection()
        if not conn:
            raise Exception("Erro de conexão com banco")

        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute("SELECT * FROM tenders WHERE id = %s", (tender_id,))
        row = cursor.fetchone()

        if not row:
            return jsonify({'success': False, 'error': 'Licitação não encontrada'}), 404

        items = []
        downloaded_files = []
        try:
            if row['items_json']:
                items = json.loads(row['items_json'])
        except:
            items = []
        try:
            if row['downloaded_files_json']:
                downloaded_files = json.loads(row['downloaded_files_json'])
        except:
            downloaded_files = []

        resolved_value = resolve_tender_value(
            row['valor_total_estimado'],
            row['estimated_value'],
            items,
        )

        tender = {
            'id': row['id'],
            'pncp_id': row['pncp_id'] or '',
            'title': row['title'] or '',
            'description': row['description'] or '',
            'organization_name': row['organization_name'] or '',
            'organization_cnpj': row['organization_cnpj'] or '',
            'municipality_name': row['municipality_name'] or '',
            'state_code': row['state_code'] or '',
            'publication_date': str(row['publication_date']) if row['publication_date'] else '',
            'publication_date_br': format_brazilian_date(row['publication_date']),
            'status': row['status'] or '',
            'modality': row['modality'] or '',
            'pncp_url': row['detail_url'] or row['source_url'] or '',
            'objeto': row['objeto'] or '',
            'prazo': row['prazo'] or '',
            'proposal_start_date': str(row['proposal_start_date']) if row.get('proposal_start_date') else '',
            'proposal_end_date': str(row['proposal_end_date']) if row.get('proposal_end_date') else '',
            'proposal_end_date_br': format_brazilian_date(row.get('proposal_end_date')),
            'is_open': tender_is_open(row.get('proposal_end_date'), row.get('publication_date'), row.get('status')),
            'days_until_close': days_until_proposal_close(row.get('proposal_end_date')),
            'proposal_close_label': proposal_close_label(row.get('proposal_end_date'), row.get('status')),
            'detailed_description': row['detailed_description'] or '',
            'valor_total_estimado': resolved_value,
            'valor_total_estimado_br': format_brazilian_currency(resolved_value),
            'formatted_value': format_brazilian_currency(resolved_value),
            'items_count': row['items_count'] or len(items),
            'downloads_count': row['downloads_count'] or 0,
            'items': items,
            'downloaded_files': downloaded_files
        }

        cursor.close()
        conn.close()

        return jsonify({'success': True, 'tender': tender})

    except Exception as e:
        logger.error(f"Error fetching tender details: {e}")
        return jsonify({'success': False, 'error': 'Erro ao buscar detalhes'}), 500


@tender_bp.route('/tenders/<int:tender_id>/download/<filename>', methods=['GET'])
def download_file(tender_id, filename):
    """Download de arquivo específico"""
    try:
        conn = get_db_connection()
        if not conn:
            raise Exception("Erro de conexão com banco")

        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute("SELECT downloaded_files_json FROM tenders WHERE id = %s", (tender_id,))
        row = cursor.fetchone()

        if not row:
            return jsonify({'error': 'Licitação não encontrada'}), 404

        try:
            downloaded_files = json.loads(row['downloaded_files_json'] or '[]')
        except:
            downloaded_files = []

        target_file = next((f for f in downloaded_files if f.get('filename') == filename), None)

        if not target_file:
            return jsonify({'error': 'Arquivo não encontrado'}), 404

        filepath = target_file.get('filepath', '')
        remote_url = target_file.get('url', '')
        if not filepath or not os.path.exists(filepath):
            if remote_url:
                cursor.close()
                conn.close()
                return jsonify({'success': True, 'redirect_url': remote_url}), 200
            return jsonify({'error': 'Arquivo não disponível no servidor'}), 404

        cursor.close()
        conn.close()

        return send_file(filepath, as_attachment=True, download_name=filename)

    except Exception as e:
        logger.error(f"Error downloading file: {e}")
        return jsonify({'error': 'Erro ao baixar arquivo'}), 500


@tender_bp.route('/tenders/<int:tender_id>/view/<filename>', methods=['GET'])
def view_file(tender_id, filename):
    """Visualizar arquivo (inline, para PDF viewer)"""
    try:
        conn = get_db_connection()
        if not conn:
            raise Exception("Erro de conexão com banco")

        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute("SELECT downloaded_files_json FROM tenders WHERE id = %s", (tender_id,))
        row = cursor.fetchone()

        if not row:
            return jsonify({'error': 'Licitação não encontrada'}), 404

        try:
            downloaded_files = json.loads(row['downloaded_files_json'] or '[]')
        except:
            downloaded_files = []

        target_file = next((f for f in downloaded_files if f.get('filename') == filename), None)

        if not target_file:
            return jsonify({'error': 'Arquivo não encontrado'}), 404

        filepath = target_file.get('filepath', '')
        remote_url = target_file.get('url', '')
        if not filepath or not os.path.exists(filepath):
            if remote_url:
                cursor.close()
                conn.close()
                return jsonify({'success': True, 'redirect_url': remote_url}), 200
            return jsonify({'error': 'Arquivo não disponível no servidor'}), 404

        cursor.close()
        conn.close()

        return send_file(filepath, as_attachment=False)

    except Exception as e:
        logger.error(f"Error viewing file: {e}")
        return jsonify({'error': 'Erro ao visualizar arquivo'}), 500


@tender_bp.route('/cities', methods=['GET'])
def get_cities():
    """Get cities usando psycopg2 diretamente"""
    try:
        conn = get_db_connection()
        if not conn:
            raise Exception("Erro de conexão com banco")

        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        query = """
        SELECT DISTINCT municipality_name, state_code, municipality_ibge, COUNT(*) as tender_count
        FROM tenders
        WHERE municipality_name IS NOT NULL AND municipality_name != ''
        GROUP BY municipality_name, state_code, municipality_ibge
        ORDER BY municipality_name
        """

        cursor.execute(query)
        rows = cursor.fetchall()

        cities = [
            {
                'name': row['municipality_name'] or '',
                'state_code': row['state_code'] or '',
                'ibge_code': row['municipality_ibge'] or '',
                'tender_count': row['tender_count'] or 0
            }
            for row in rows
        ]

        cursor.close()
        conn.close()

        return jsonify({'success': True, 'cities': cities})

    except Exception as e:
        logger.error(f"Error fetching cities: {e}")
        return jsonify({'success': False, 'error': 'Erro ao buscar cidades', 'cities': []}), 500


@tender_bp.route('/states', methods=['GET'])
def get_states():
    """Get states usando psycopg2 diretamente"""
    try:
        conn = get_db_connection()
        if not conn:
            raise Exception("Erro de conexão com banco")

        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        query = """
        SELECT state_code, COUNT(*) as tender_count
        FROM tenders
        WHERE state_code IS NOT NULL AND state_code != ''
        GROUP BY state_code
        ORDER BY state_code
        """

        cursor.execute(query)
        rows = cursor.fetchall()

        states = [
            {'code': row['state_code'] or '', 'name': row['state_code'] or '', 'count': row['tender_count'] or 0}
            for row in rows
        ]

        cursor.close()
        conn.close()

        return jsonify({'success': True, 'states': states})

    except Exception as e:
        logger.error(f"Error fetching states: {e}")
        return jsonify({'success': False, 'error': 'Erro ao buscar estados', 'states': []}), 500


def _load_subscription_filters(user_id):
    """Carrega estados e áreas da assinatura ativa do usuário."""
    conn = get_db_connection()
    if not conn:
        return None, None

    try:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute("""
            SELECT selected_states, selected_areas
            FROM subscriptions
            WHERE user_id = %s AND status = 'active'
            ORDER BY created_at DESC
            LIMIT 1
        """, (user_id,))
        row = cursor.fetchone()
        cursor.close()
        conn.close()

        if not row:
            return None, None

        states = parse_json_list(row['selected_states'])
        areas = parse_json_list(row['selected_areas'])
        return states or None, areas or None
    except Exception as e:
        logger.error(f"Erro ao carregar subscription para stats: {e}")
        return None, None


@tender_bp.route('/stats', methods=['GET'])
def get_stats():
    """Retorna estatísticas do sistema, opcionalmente filtradas pelo plano do usuário."""
    try:
        user_id = request.args.get('user_id', type=int)
        scoped = False
        filter_clause = ""
        filter_params = []

        if user_id:
            states, areas = _load_subscription_filters(user_id)
            if states or areas:
                filter_clause, filter_params = build_plan_filter(states, areas)
                scoped = bool(filter_clause)

        where_clause = f" WHERE {filter_clause}" if filter_clause else ""

        conn = get_db_connection()
        if not conn:
            raise Exception("Erro de conexão com banco")

        cursor = conn.cursor()

        cursor.execute(f"SELECT COUNT(*) FROM tenders{where_clause}", filter_params)
        total_tenders = cursor.fetchone()[0]

        cities_where = where_clause
        cities_params = list(filter_params)
        if cities_where:
            cities_where += " AND municipality_name IS NOT NULL"
        else:
            cities_where = " WHERE municipality_name IS NOT NULL"
        cursor.execute(f"SELECT COUNT(DISTINCT municipality_name) FROM tenders{cities_where}", cities_params)
        total_cities = cursor.fetchone()[0]

        items_where = where_clause
        items_params = list(filter_params)
        if items_where:
            items_where += " AND items_count IS NOT NULL"
        else:
            items_where = " WHERE items_count IS NOT NULL"
        cursor.execute(f"SELECT COALESCE(SUM(items_count), 0) FROM tenders{items_where}", items_params)
        total_items = cursor.fetchone()[0]

        files_where = where_clause
        files_params = list(filter_params)
        if files_where:
            files_where += " AND downloads_count IS NOT NULL"
        else:
            files_where = " WHERE downloads_count IS NOT NULL"
        cursor.execute(f"SELECT COALESCE(SUM(downloads_count), 0) FROM tenders{files_where}", files_params)
        total_files = cursor.fetchone()[0]

        states_where = where_clause
        states_params = list(filter_params)
        if states_where:
            states_where += " AND state_code IS NOT NULL"
        else:
            states_where = " WHERE state_code IS NOT NULL"
        cursor.execute(f"SELECT COUNT(DISTINCT state_code) FROM tenders{states_where}", states_params)
        total_states = cursor.fetchone()[0]

        value_where = where_clause
        value_params = list(filter_params)
        if value_where:
            value_where += " AND estimated_value IS NOT NULL"
        else:
            value_where = " WHERE estimated_value IS NOT NULL"
        cursor.execute(f"SELECT COALESCE(SUM(estimated_value), 0) FROM tenders{value_where}", value_params)
        total_value = cursor.fetchone()[0]

        cursor.close()
        conn.close()

        stats = {
            'total_tenders': total_tenders,
            'total_cities': total_cities,
            'total_items': int(total_items) if total_items else 0,
            'total_files': int(total_files) if total_files else 0,
            'total_states': total_states,
            'total_value': float(total_value) if total_value else 0.0,
            'formatted_value': f"R$ {float(total_value):,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.') if total_value else "R$ 0,00",
            'scoped': scoped,
        }

        return jsonify({'success': True, 'stats': stats})

    except Exception as e:
        logger.error(f"Erro ao buscar estatísticas: {e}")
        return jsonify({
            'success': False,
            'error': str(e),
            'stats': {
                'total_tenders': 0, 'total_cities': 0, 'total_items': 0,
                'total_files': 0, 'total_states': 0, 'total_value': 0.0,
                'formatted_value': "R$ 0,00", 'scoped': False,
            }
        }), 500


@tender_bp.route('/test', methods=['GET'])
def test_connection():
    """Test usando psycopg2 diretamente"""
    try:
        conn = get_db_connection()
        if not conn:
            raise Exception("Erro de conexão com banco")

        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM tenders")
        count = cursor.fetchone()[0]

        cursor.close()
        conn.close()

        return jsonify({
            'success': True,
            'message': 'API melhorada funcionando!',
            'total_tenders': count,
            'method': 'psycopg2_direct_enhanced_v2',
            'features': ['formatacao_brasileira', 'dados_completos', 'download_arquivos',
                         'multiplos_estados', 'multiplas_keywords', 'filtro_valor', 'filtro_hoje']
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500