"""Marca licitações vencidas como Expirada."""

import logging
import os

import psycopg2
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)


def expirar_licitacoes_encerradas() -> int:
    """
    Atualiza status para 'Expirada' quando proposal_end_date < hoje.
    Retorna quantidade de registros atualizados.
    """
    conn = psycopg2.connect(
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT', 5432),
        database=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        client_encoding='utf8',
    )
    try:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE tenders
            SET status = 'Expirada'
            WHERE proposal_end_date IS NOT NULL
              AND proposal_end_date < CURRENT_DATE
              AND LOWER(COALESCE(status, '')) NOT IN (
                'expirada', 'cancelado', 'encerrada', 'encerrado', 'finalizada'
              )
        """)
        count = cursor.rowcount
        conn.commit()
        cursor.close()
        if count:
            logger.info('Marcadas %s licitação(ões) como Expirada', count)
        return count
    finally:
        conn.close()
