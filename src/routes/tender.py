# -*- coding: utf-8 -*-
"""
API direta com psycopg2 - contorna problemas do SQLAlchemy
"""

from flask import Blueprint, request, jsonify
import psycopg2
import psycopg2.extras
import logging
import json
import os
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

@tender_bp.route('/tenders', methods=['GET'])
def get_tenders():
    """Get tenders usando psycopg2 diretamente"""
    try:
        # Parâmetros
        page = request.args.get('page', 1, type=int)
        per_page = min(request.args.get('per_page', 10, type=int), 100)
        city_name = request.args.get('city_name', '').strip()
        state_code = request.args.get('state_code', '').strip()
        keyword = request.args.get('keyword', '').strip()

        # Conectar
        conn = get_db_connection()
        if not conn:
            raise Exception("Erro de conexão com banco")

        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # Query base
        base_query = """
        SELECT id, pncp_id, title, description, organization_name, organization_cnpj,
               municipality_name, municipality_ibge, state_code, publication_date,
               status, modality, estimated_value, source_url, detail_url,
               data_source, created_at, downloaded_files
        FROM tenders 
        WHERE 1=1
        """

        params = []

        # Aplicar filtros
        if city_name:
            base_query += " AND municipality_name ILIKE %s"
            params.append(f'%{city_name}%')

        if state_code:
            base_query += " AND state_code ILIKE %s"
            params.append(f'%{state_code}%')

        if keyword:
            base_query += " AND (title ILIKE %s OR description ILIKE %s OR organization_name ILIKE %s)"
            params.extend([f'%{keyword}%', f'%{keyword}%', f'%{keyword}%'])

        # Ordenar e paginar
        base_query += " ORDER BY publication_date DESC"
        base_query += f" LIMIT {per_page} OFFSET {(page - 1) * per_page}"

        # Executar query
        cursor.execute(base_query, params)
        rows = cursor.fetchall()

        # Converter para lista de dicionários
        tenders = []
        for row in rows:
            tender_dict = {
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
                'status': row['status'] or '',
                'modality': row['modality'] or '',
                'estimated_value': float(row['estimated_value']) if row['estimated_value'] else None,
                'source_url': row['source_url'] or '',
                'detail_url': row['detail_url'] or '',
                'data_source': row['data_source'] or '',
                'created_at': str(row['created_at']) if row['created_at'] else '',
                'downloaded_files': row['downloaded_files'] or '[]',
                'pncp_url': row['detail_url'] or row['source_url'] or '',
                'formatted_value': f"R$ {float(row['estimated_value']):,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.') if row['estimated_value'] else 'Valor não informado'
            }
            tenders.append(tender_dict)

        # Contar total
        count_query = "SELECT COUNT(*) FROM tenders WHERE 1=1"
        count_params = []

        if city_name:
            count_query += " AND municipality_name ILIKE %s"
            count_params.append(f'%{city_name}%')

        if state_code:
            count_query += " AND state_code ILIKE %s"
            count_params.append(f'%{state_code}%')

        if keyword:
            count_query += " AND (title ILIKE %s OR description ILIKE %s OR organization_name ILIKE %s)"
            count_params.extend([f'%{keyword}%', f'%{keyword}%', f'%{keyword}%'])

        cursor.execute(count_query, count_params)
        total = cursor.fetchone()['count']

        # Fechar conexão
        cursor.close()
        conn.close()

        # Calcular paginação
        pages = (total + per_page - 1) // per_page
        has_next = page < pages
        has_prev = page > 1

        return jsonify({
            'success': True,
            'tenders': tenders,
            'pagination': {
                'page': page,
                'pages': pages,
                'per_page': per_page,
                'total': total,
                'has_next': has_next,
                'has_prev': has_prev
            },
            'filters_applied': {
                'city_name': city_name,
                'state_code': state_code,
                'keyword': keyword
            }
        })

    except Exception as e:
        logger.error(f"Error fetching tenders: {e}")
        return jsonify({
            'success': False,
            'error': 'Erro interno do servidor',
            'tenders': [],
            'pagination': {
                'page': 1,
                'pages': 0,
                'per_page': 10,
                'total': 0,
                'has_next': False,
                'has_prev': False
            }
        }), 500

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

        cities = []
        for row in rows:
            city = {
                'name': row['municipality_name'] or '',
                'state_code': row['state_code'] or '',
                'ibge_code': row['municipality_ibge'] or '',
                'tender_count': row['tender_count'] or 0
            }
            cities.append(city)

        cursor.close()
        conn.close()

        return jsonify({
            'success': True,
            'cities': cities
        })

    except Exception as e:
        logger.error(f"Error fetching cities: {e}")
        return jsonify({
            'success': False,
            'error': 'Erro ao buscar cidades',
            'cities': []
        }), 500

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

        states = []
        for row in rows:
            state = {
                'code': row['state_code'] or '',
                'name': row['state_code'] or '',
                'count': row['tender_count'] or 0
            }
            states.append(state)

        cursor.close()
        conn.close()

        return jsonify({
            'success': True,
            'states': states
        })

    except Exception as e:
        logger.error(f"Error fetching states: {e}")
        return jsonify({
            'success': False,
            'error': 'Erro ao buscar estados',
            'states': []
        }), 500

@tender_bp.route('/stats', methods=['GET'])
def get_stats():
    """Get stats usando psycopg2 diretamente"""
    try:
        conn = get_db_connection()
        if not conn:
            raise Exception("Erro de conexão com banco")

        cursor = conn.cursor()

        # Total de licitações
        cursor.execute("SELECT COUNT(*) FROM tenders")
        total_tenders = cursor.fetchone()[0]

        # Total de cidades
        cursor.execute("SELECT COUNT(DISTINCT municipality_name) FROM tenders WHERE municipality_name IS NOT NULL")
        total_cities = cursor.fetchone()[0]

        # Total de estados
        cursor.execute("SELECT COUNT(DISTINCT state_code) FROM tenders WHERE state_code IS NOT NULL")
        total_states = cursor.fetchone()[0]

        cursor.close()
        conn.close()

        return jsonify({
            'success': True,
            'stats': {
                'total_tenders': total_tenders,
                'total_cities': total_cities,
                'total_states': total_states,
                'last_update': '2025-08-26'
            }
        })

    except Exception as e:
        logger.error(f"Error fetching stats: {e}")
        return jsonify({
            'success': False,
            'error': 'Erro ao buscar estatísticas',
            'stats': {
                'total_tenders': 0,
                'total_cities': 0,
                'total_states': 0,
                'last_update': '2025-08-26'
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
            'message': 'API direta funcionando!',
            'total_tenders': count,
            'method': 'psycopg2_direct'
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500
