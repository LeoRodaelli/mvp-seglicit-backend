# -*- coding: utf-8 -*-
"""
API melhorada com dados completos e formatação brasileira
"""

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
            # Se já está no formato YYYY-MM-DD
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
        # Converter para float se necessário
        if isinstance(value, str):
            value = float(value.replace(',', '.'))

        # Formatar como moeda brasileira
        formatted = f"R$ {float(value):,.2f}"
        # Trocar . por , e , por .
        formatted = formatted.replace(',', 'TEMP').replace('.', ',').replace('TEMP', '.')
        return formatted
    except:
        return 'Valor não informado'

@tender_bp.route('/tenders', methods=['GET'])
def get_tenders():
    """Get tenders com dados completos e formatação brasileira"""
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

        # Query com TODOS os campos
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

        # Aplicar filtros
        if city_name:
            base_query += " AND municipality_name ILIKE %s"
            params.append(f'%{city_name}%')

        if state_code:
            base_query += " AND state_code ILIKE %s"
            params.append(f'%{state_code}%')

        if keyword:
            base_query += " AND (title ILIKE %s OR description ILIKE %s OR organization_name ILIKE %s OR objeto ILIKE %s)"
            params.extend([f'%{keyword}%', f'%{keyword}%', f'%{keyword}%', f'%{keyword}%'])

        # Ordenar e paginar
        base_query += " ORDER BY publication_date DESC"
        base_query += f" LIMIT {per_page} OFFSET {(page - 1) * per_page}"

        # Executar query
        cursor.execute(base_query, params)
        rows = cursor.fetchall()

        # Converter para lista de dicionários com formatação brasileira
        tenders = []
        for row in rows:
            # Parse JSON fields
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
                'publication_date_br': format_brazilian_date(row['publication_date']),
                'status': row['status'] or '',
                'modality': row['modality'] or '',
                'estimated_value': float(row['estimated_value']) if row['estimated_value'] else None,
                'source_url': row['source_url'] or '',
                'detail_url': row['detail_url'] or '',
                'data_source': row['data_source'] or '',
                'created_at': str(row['created_at']) if row['created_at'] else '',
                'pncp_url': row['detail_url'] or row['source_url'] or '',

                # NOVOS CAMPOS COMPLETOS
                'objeto': row['objeto'] or '',
                'prazo': row['prazo'] or '',
                'detailed_description': row['detailed_description'] or '',
                'valor_total_estimado': float(row['valor_total_estimado']) if row['valor_total_estimado'] else None,
                'valor_total_estimado_br': format_brazilian_currency(row['valor_total_estimado']),
                'items_count': row['items_count'] or 0,
                'downloads_count': row['downloads_count'] or 0,
                'items': items,
                'downloaded_files': downloaded_files,

                # FORMATAÇÃO BRASILEIRA
                'formatted_value': format_brazilian_currency(row['valor_total_estimado'] or row['estimated_value']),
                'has_items': len(items) > 0,
                'has_files': len(downloaded_files) > 0
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
            count_query += " AND (title ILIKE %s OR description ILIKE %s OR organization_name ILIKE %s OR objeto ILIKE %s)"
            count_params.extend([f'%{keyword}%', f'%{keyword}%', f'%{keyword}%', f'%{keyword}%'])

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

@tender_bp.route('/tenders/<int:tender_id>', methods=['GET'])
def get_tender_details(tender_id):
    """Get detalhes completos de uma licitação"""
    try:
        conn = get_db_connection()
        if not conn:
            raise Exception("Erro de conexão com banco")

        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        query = """
        SELECT * FROM tenders WHERE id = %s
        """

        cursor.execute(query, (tender_id,))
        row = cursor.fetchone()

        if not row:
            return jsonify({
                'success': False,
                'error': 'Licitação não encontrada'
            }), 404

        # Parse JSON fields
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
            'publication_date_br': format_brazilian_date(row['publication_date']),
            'status': row['status'] or '',
            'modality': row['modality'] or '',
            'pncp_url': row['detail_url'] or row['source_url'] or '',
            'objeto': row['objeto'] or '',
            'prazo': row['prazo'] or '',
            'detailed_description': row['detailed_description'] or '',
            'valor_total_estimado_br': format_brazilian_currency(row['valor_total_estimado']),
            'items_count': row['items_count'] or 0,
            'downloads_count': row['downloads_count'] or 0,
            'items': items,
            'downloaded_files': downloaded_files
        }

        cursor.close()
        conn.close()

        return jsonify({
            'success': True,
            'tender': tender
        })

    except Exception as e:
        logger.error(f"Error fetching tender details: {e}")
        return jsonify({
            'success': False,
            'error': 'Erro ao buscar detalhes'
        }), 500

@tender_bp.route('/tenders/<int:tender_id>/download/<filename>', methods=['GET'])
def download_file(tender_id, filename):
    """Download de arquivo específico"""
    try:
        # Buscar informações do arquivo
        conn = get_db_connection()
        if not conn:
            raise Exception("Erro de conexão com banco")

        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        query = "SELECT downloaded_files_json FROM tenders WHERE id = %s"
        cursor.execute(query, (tender_id,))
        row = cursor.fetchone()

        if not row:
            return jsonify({'error': 'Licitação não encontrada'}), 404

        # Parse arquivos
        try:
            downloaded_files = json.loads(row['downloaded_files_json'] or '[]')
        except:
            downloaded_files = []

        # Buscar arquivo específico
        target_file = None
        for file_info in downloaded_files:
            if file_info.get('filename') == filename:
                target_file = file_info
                break

        if not target_file:
            return jsonify({'error': 'Arquivo não encontrado'}), 404

        # Verificar se arquivo existe no disco
        filepath = target_file.get('filepath', '')
        if not os.path.exists(filepath):
            return jsonify({'error': 'Arquivo não disponível no servidor'}), 404

        cursor.close()
        conn.close()

        # Enviar arquivo
        return send_file(
            filepath,
            as_attachment=True,
            download_name=filename
        )

    except Exception as e:
        logger.error(f"Error downloading file: {e}")
        return jsonify({'error': 'Erro ao baixar arquivo'}), 500

# Manter rotas existentes (cities, states, stats, test)
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
    """Retorna estatísticas gerais do sistema"""
    try:
        conn = get_db_connection()
        if not conn:
            raise Exception("Erro de conexão com banco")

        cursor = conn.cursor()

        # Total de licitações
        cursor.execute("SELECT COUNT(*) FROM tenders")
        total_tenders = cursor.fetchone()[0]

        # Total de cidades únicas
        cursor.execute("SELECT COUNT(DISTINCT municipality_name) FROM tenders WHERE municipality_name IS NOT NULL")
        total_cities = cursor.fetchone()[0]

        # Total de itens (somar items_count)
        cursor.execute("SELECT COALESCE(SUM(items_count), 0) FROM tenders WHERE items_count IS NOT NULL")
        total_items = cursor.fetchone()[0]

        # Total de arquivos (somar downloads_count)
        cursor.execute("SELECT COALESCE(SUM(downloads_count), 0) FROM tenders WHERE downloads_count IS NOT NULL")
        total_files = cursor.fetchone()[0]

        # Estados únicos
        cursor.execute("SELECT COUNT(DISTINCT state_code) FROM tenders WHERE state_code IS NOT NULL")
        total_states = cursor.fetchone()[0]

        # Valor total estimado
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

        return jsonify({
            'success': True,
            'stats': stats
        })

    except Exception as e:
        logger.error(f"Erro ao buscar estatísticas: {e}")
        return jsonify({
            'success': False,
            'error': str(e),
            'stats': {
                'total_tenders': 0,
                'total_cities': 0,
                'total_items': 0,
                'total_files': 0,
                'total_states': 0,
                'total_value': 0.0,
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
            'method': 'psycopg2_direct_enhanced',
            'features': ['formatacao_brasileira', 'dados_completos', 'download_arquivos']
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500
