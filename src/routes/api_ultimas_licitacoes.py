"""
API Route: Últimas Licitações (Real-time Updates)
Adicionar ao backend Flask/FastAPI
"""

from flask import Blueprint, jsonify, request
from datetime import datetime, timedelta
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

# Criar blueprint
licitacoes_realtime_bp = Blueprint('licitacoes_realtime', __name__)


def get_db_connection():
    """Cria conexão com banco de dados"""
    return psycopg2.connect(
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT', 5432),
        database=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
    )


@licitacoes_realtime_bp.route('/licitacoes/ultimas', methods=['GET'])
def get_ultimas_licitacoes():
    """
    Retorna as últimas licitações adicionadas
    
    Query params:
    - limit: número de licitações (padrão: 10)
    - since: timestamp ISO 8601 (retorna apenas após este horário)
    - minutes: minutos atrás (alternativa ao since)
    """
    try:
        # Parâmetros
        limit = int(request.args.get('limit', 10))
        since = request.args.get('since')  # ISO 8601: 2026-01-07T16:00:00
        minutes = request.args.get('minutes')  # Ex: 30 (últimos 30 minutos)
        
        # Conectar ao banco
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Construir query
        query = """
            SELECT 
                id,
                pncp_id,
                title,
                description,
                organization_name,
                municipality_name,
                state_code,
                publication_date,
                status,
                modality,
                estimated_value,
                valor_total_estimado,
                items_count,
                downloads_count,
                created_at,
                objeto
            FROM tenders
            WHERE 1=1
        """
        params = []
        
        # Filtro por tempo
        if since:
            query += " AND created_at > %s"
            params.append(since)
        elif minutes:
            query += " AND created_at > NOW() - INTERVAL '%s minutes'"
            params.append(int(minutes))
        
        # Ordenar por mais recente
        query += " ORDER BY created_at DESC LIMIT %s"
        params.append(limit)
        
        cursor.execute(query, params)
        rows = cursor.fetchall()
        
        # Formatar resultado
        licitacoes = []
        for row in rows:
            licitacoes.append({
                'id': row[0],
                'pncp_id': row[1],
                'title': row[2],
                'description': row[3],
                'organization_name': row[4],
                'municipality_name': row[5],
                'state_code': row[6],
                'publication_date': row[7].isoformat() if row[7] else None,
                'status': row[8],
                'modality': row[9],
                'estimated_value': float(row[10]) if row[10] else None,
                'valor_total_estimado': float(row[11]) if row[11] else None,
                'items_count': row[12],
                'downloads_count': row[13],
                'created_at': row[14].isoformat() if row[14] else None,
                'objeto': row[15],
                'is_new': True  # Flag para destacar no frontend
            })
        
        conn.close()
        
        return jsonify({
            'success': True,
            'count': len(licitacoes),
            'licitacoes': licitacoes,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@licitacoes_realtime_bp.route('/licitacoes/stats', methods=['GET'])
def get_stats():
    """
    Retorna estatísticas das licitações
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Total de licitações
        cursor.execute("SELECT COUNT(*) FROM tenders")
        total = cursor.fetchone()[0]
        
        # Novas hoje
        cursor.execute("""
            SELECT COUNT(*) FROM tenders 
            WHERE DATE(created_at) = CURRENT_DATE
        """)
        novas_hoje = cursor.fetchone()[0]
        
        # Novas última hora
        cursor.execute("""
            SELECT COUNT(*) FROM tenders 
            WHERE created_at > NOW() - INTERVAL '1 hour'
        """)
        novas_ultima_hora = cursor.fetchone()[0]
        
        # Valor total
        cursor.execute("""
            SELECT SUM(COALESCE(valor_total_estimado, estimated_value, 0)) 
            FROM tenders
        """)
        valor_total = cursor.fetchone()[0] or 0
        
        # Última atualização
        cursor.execute("""
            SELECT MAX(created_at) FROM tenders
        """)
        ultima_atualizacao = cursor.fetchone()[0]
        
        # Próxima execução (da tabela automation_logs)
        cursor.execute("""
            SELECT execution_date, new_tenders 
            FROM automation_logs 
            WHERE status = 'success'
            ORDER BY execution_date DESC 
            LIMIT 1
        """)
        last_run = cursor.fetchone()
        
        conn.close()
        
        return jsonify({
            'success': True,
            'stats': {
                'total': total,
                'novas_hoje': novas_hoje,
                'novas_ultima_hora': novas_ultima_hora,
                'valor_total': float(valor_total),
                'ultima_atualizacao': ultima_atualizacao.isoformat() if ultima_atualizacao else None,
                'ultima_execucao': {
                    'data': last_run[0].isoformat() if last_run else None,
                    'novas': last_run[1] if last_run else 0
                } if last_run else None
            }
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@licitacoes_realtime_bp.route('/licitacoes/check-new', methods=['POST'])
def check_new():
    """
    Verifica se há novas licitações desde o último check
    
    Body:
    {
        "last_check": "2026-01-07T16:00:00",
        "last_id": 123
    }
    """
    try:
        data = request.get_json()
        last_check = data.get('last_check')
        last_id = data.get('last_id')
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Verificar novas licitações
        query = "SELECT COUNT(*) FROM tenders WHERE 1=1"
        params = []
        
        if last_check:
            query += " AND created_at > %s"
            params.append(last_check)
        
        if last_id:
            query += " AND id > %s"
            params.append(last_id)
        
        cursor.execute(query, params)
        new_count = cursor.fetchone()[0]
        
        # Se houver novas, buscar IDs
        new_ids = []
        if new_count > 0:
            query = "SELECT id FROM tenders WHERE 1=1"
            params = []
            
            if last_check:
                query += " AND created_at > %s"
                params.append(last_check)
            
            if last_id:
                query += " AND id > %s"
                params.append(last_id)
            
            query += " ORDER BY created_at DESC"
            
            cursor.execute(query, params)
            new_ids = [row[0] for row in cursor.fetchall()]
        
        conn.close()
        
        return jsonify({
            'success': True,
            'has_new': new_count > 0,
            'new_count': new_count,
            'new_ids': new_ids,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


# ============================================================
# INTEGRAÇÃO NO APP PRINCIPAL
# ============================================================

"""
# No arquivo principal do backend (app.py ou main.py):

from api_ultimas_licitacoes import licitacoes_realtime_bp

# Registrar blueprint
app.register_blueprint(licitacoes_realtime_bp)

# Ou se usar FastAPI, converter para rotas FastAPI
"""
