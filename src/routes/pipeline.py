# -*- coding: utf-8 -*-
"""
Pipeline de acompanhamento de licitações (mini-CRM): cada usuário pode
marcar uma licitação com um status ("interessado", "em_analise",
"proposta_enviada", "ganhou", "perdeu") e uma nota livre, pra organizar
quais licitações está de fato disputando — diferente de "Favoritos"
(que é só uma lista salva no navegador, sem status nem sincronização
entre dispositivos).
"""
from flask import Blueprint, request, jsonify
import psycopg2
import psycopg2.extras
import logging
import os
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

pipeline_bp = Blueprint('pipeline', __name__)

STATUS_VALIDOS = {'interessado', 'em_analise', 'proposta_enviada', 'ganhou', 'perdeu'}


def get_db_connection():
    try:
        return psycopg2.connect(
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT', 5432),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            client_encoding='utf8',
        )
    except Exception as e:
        logger.error(f"Pipeline: erro de conexão com banco: {e}")
        return None


def _ensure_table(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS tender_pipeline (
            id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL,
            tender_id INTEGER NOT NULL,
            status VARCHAR(20) NOT NULL DEFAULT 'interessado',
            notes TEXT,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW(),
            UNIQUE(user_id, tender_id)
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_tender_pipeline_user ON tender_pipeline (user_id)")


@pipeline_bp.route('/pipeline', methods=['GET'])
def list_pipeline():
    """Lista o pipeline do usuário, já com os dados da licitação."""
    user_id = request.args.get('user_id', type=int)
    if not user_id:
        return jsonify({'success': False, 'error': 'user_id obrigatório'}), 400

    conn = get_db_connection()
    if not conn:
        return jsonify({'success': False, 'error': 'Erro de conexão com banco'}), 500

    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    _ensure_table(cursor)
    conn.commit()

    cursor.execute("""
        SELECT
            p.id, p.tender_id, p.status, p.notes, p.created_at, p.updated_at,
            t.title, t.organization_name, t.municipality_name, t.state_code,
            t.estimated_value, t.valor_total_estimado, t.publication_date,
            t.proposal_end_date, t.detail_url, t.source_url,
            t.status AS tender_status
        FROM tender_pipeline p
        JOIN tenders t ON t.id = p.tender_id
        WHERE p.user_id = %s
        ORDER BY p.updated_at DESC
    """, (user_id,))
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    items = []
    for row in rows:
        item = dict(row)
        proposal_end = item.get('proposal_end_date')
        if proposal_end:
            try:
                item['proposal_end_date_br'] = proposal_end.strftime('%d/%m/%Y')
            except Exception:
                item['proposal_end_date_br'] = None
        else:
            item['proposal_end_date_br'] = None
        for date_field in ('created_at', 'updated_at', 'publication_date', 'proposal_end_date'):
            if item.get(date_field):
                item[date_field] = str(item[date_field])
        item['valor'] = item.get('valor_total_estimado') or item.get('estimated_value')
        item['link'] = item.get('detail_url') or item.get('source_url')
        items.append(item)

    return jsonify({'success': True, 'items': items})


@pipeline_bp.route('/pipeline/<int:tender_id>', methods=['PUT'])
def upsert_pipeline(tender_id):
    """Adiciona a licitação ao pipeline, ou atualiza status/nota se já estiver lá."""
    data = request.get_json() or {}
    user_id = data.get('user_id')
    status = (data.get('status') or 'interessado').strip()
    notes = data.get('notes')

    if not user_id:
        return jsonify({'success': False, 'error': 'user_id obrigatório'}), 400
    if status not in STATUS_VALIDOS:
        return jsonify({'success': False, 'error': f'status inválido: {status}'}), 400

    conn = get_db_connection()
    if not conn:
        return jsonify({'success': False, 'error': 'Erro de conexão com banco'}), 500

    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    _ensure_table(cursor)

    cursor.execute("""
        INSERT INTO tender_pipeline (user_id, tender_id, status, notes, updated_at)
        VALUES (%s, %s, %s, %s, NOW())
        ON CONFLICT (user_id, tender_id) DO UPDATE SET
            status = EXCLUDED.status,
            notes = COALESCE(EXCLUDED.notes, tender_pipeline.notes),
            updated_at = NOW()
        RETURNING id, user_id, tender_id, status, notes, created_at, updated_at
    """, (user_id, tender_id, status, notes))
    row = cursor.fetchone()
    conn.commit()
    cursor.close()
    conn.close()

    item = dict(row)
    for date_field in ('created_at', 'updated_at'):
        if item.get(date_field):
            item[date_field] = str(item[date_field])

    return jsonify({'success': True, 'item': item})


@pipeline_bp.route('/pipeline/<int:tender_id>', methods=['DELETE'])
def remove_from_pipeline(tender_id):
    """Remove a licitação do pipeline do usuário."""
    user_id = request.args.get('user_id', type=int)
    if not user_id:
        return jsonify({'success': False, 'error': 'user_id obrigatório'}), 400

    conn = get_db_connection()
    if not conn:
        return jsonify({'success': False, 'error': 'Erro de conexão com banco'}), 500

    cursor = conn.cursor()
    _ensure_table(cursor)
    cursor.execute(
        "DELETE FROM tender_pipeline WHERE user_id = %s AND tender_id = %s",
        (user_id, tender_id),
    )
    conn.commit()
    cursor.close()
    conn.close()

    return jsonify({'success': True})
