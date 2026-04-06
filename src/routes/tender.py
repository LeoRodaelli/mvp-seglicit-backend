from flask import Blueprint, request, jsonify, send_file
import psycopg2
import psycopg2.extras
import logging
import json
import os
from datetime import datetime
from dotenv import load_dotenv

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
        'detailed_description': row['detailed_description'] or '',
        'valor_total_estimado': float(row['valor_total_estimado']) if row['valor_total_estimado'] else None,
        'valor_total_estimado_br': format_brazilian_currency(row['valor_total_estimado']),
        'items_count': row['items_count'] or 0,
        'downloads_count': row['downloads_count'] or 0,
        'items': items,
        'downloaded_files': downloaded_files,
        'formatted_value': format_brazilian_currency(row['valor_total_estimado'] or row['estimated_value']),
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
      - date_from           : data inicial do período (YYYY-MM-DD)
      - date_to             : data final do período (YYYY-MM-DD)
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
        date_from = request.args.get('date_from', '').strip()   # YYYY-MM-DD
        date_to = request.args.get('date_to', '').strip()       # YYYY-MM-DD

        # ── Filtro por IDs específicos (para favoritos) ──────────────────────────────
        # Aceita: ids=1&ids=2&ids=3  OU  ids=1,2,3
        raw_ids = request.args.getlist('ids')
        filter_ids = []
        for i in raw_ids:
            for part in i.split(','):
                part = part.strip()
                if part.isdigit():
                    filter_ids.append(int(part))

        # ── Múltiplos estados ──────────────────────────────────────────────
        # Aceita: state_code=SP&state_code=RJ  OU  state_code=SP,RJ
        raw_states = request.args.getlist('state_code')
        state_codes = []
        for s in raw_states:
            for part in s.split(','):
                part = part.strip().upper()
                if part:
                    state_codes.append(part)

        # ── Múltiplas keywords ─────────────────────────────────────────────
        # Aceita: keywords=computador,notebook,servidor
        # OU keyword=computador (retrocompatível)
        all_keywords = []
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
               items_count, downloads_count
        FROM tenders
        WHERE 1=1
        """
        params = []

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

        # Filtro por múltiplas keywords (OR entre elas, busca em title + objeto + description)
        if all_keywords:
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

        if all_keywords:
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
                'apenas_hoje': apenas_hoje
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
            'detailed_description': row['detailed_description'] or '',
            'valor_total_estimado': float(row['valor_total_estimado']) if row['valor_total_estimado'] else None,
            'valor_total_estimado_br': format_brazilian_currency(row['valor_total_estimado']),
            'formatted_value': format_brazilian_currency(row['valor_total_estimado'] or row['estimated_value']),
            'items_count': row['items_count'] or 0,
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
        if not os.path.exists(filepath):
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
        if not os.path.exists(filepath):
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


@tender_bp.route('/stats', methods=['GET'])
def get_stats():
    """Retorna estatísticas gerais do sistema"""
    try:
        conn = get_db_connection()
        if not conn:
            raise Exception("Erro de conexão com banco")

        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM tenders")
        total_tenders = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(DISTINCT municipality_name) FROM tenders WHERE municipality_name IS NOT NULL")
        total_cities = cursor.fetchone()[0]

        cursor.execute("SELECT COALESCE(SUM(items_count), 0) FROM tenders WHERE items_count IS NOT NULL")
        total_items = cursor.fetchone()[0]

        cursor.execute("SELECT COALESCE(SUM(downloads_count), 0) FROM tenders WHERE downloads_count IS NOT NULL")
        total_files = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(DISTINCT state_code) FROM tenders WHERE state_code IS NOT NULL")
        total_states = cursor.fetchone()[0]

        cursor.execute("SELECT COALESCE(SUM(estimated_value), 0) FROM tenders WHERE estimated_value IS NOT NULL")
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
            'formatted_value': f"R$ {float(total_value):,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.') if total_value else "R$ 0,00"
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
                'formatted_value': "R$ 0,00"
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