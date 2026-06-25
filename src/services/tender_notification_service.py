# -*- coding: utf-8 -*-
"""
Notifica assinantes ativos quando licitações novas correspondem ao plano
(estados + áreas de atuação).

Modo padrão: email RESUMO (1 email por usuário por lote com todas as licitações).
Também dispara webhook Zaia por licitação, se configurado.
"""
import logging
import os
import threading
import time
from datetime import datetime

import psycopg2
import psycopg2.extras
import requests as http_requests

from src.utils.plan_filters import get_keywords_for_areas, parse_json_list

logger = logging.getLogger(__name__)

EMAIL_RATE_DELAY = float(os.getenv('EMAIL_RATE_LIMIT_DELAY', '0.25'))


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
    except Exception as exc:
        logger.error(f"Notificação: erro de conexão com banco: {exc}")
        return None


def _ensure_notification_table(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS tender_email_notifications (
            id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL,
            tender_id INTEGER NOT NULL,
            sent_at TIMESTAMP DEFAULT NOW(),
            UNIQUE(user_id, tender_id)
        )
    """)


def _format_currency(value):
    if not value:
        return 'Valor não informado'
    try:
        val = float(value)
        formatted = f"R$ {val:,.2f}"
        return formatted.replace(',', 'TEMP').replace('.', ',').replace('TEMP', '.')
    except Exception:
        return 'Valor não informado'


def tender_matches_plan(licitacao_dict, states, areas):
    """Verifica se a licitação bate com estados e áreas do plano."""
    estado = (
        licitacao_dict.get('state_code')
        or licitacao_dict.get('estado')
        or ''
    ).upper()

    parsed_states = [s.strip().upper() for s in (states or []) if s and str(s).strip()]
    if parsed_states and estado not in parsed_states:
        return False

    parsed_areas = areas or []
    if not parsed_areas:
        return True

    keywords = get_keywords_for_areas(parsed_areas)
    if not keywords:
        return True

    texto = ' '.join([
        licitacao_dict.get('title') or licitacao_dict.get('titulo') or '',
        licitacao_dict.get('objeto') or licitacao_dict.get('description') or '',
        licitacao_dict.get('description') or licitacao_dict.get('descricao') or '',
    ]).lower()

    return any(kw in texto for kw in keywords)


def _load_active_subscribers(cursor):
    cursor.execute("""
        SELECT DISTINCT ON (u.id)
            u.id, u.full_name, u.email, u.zaia_webhook_url,
            s.selected_states, s.selected_areas, s.plan_name
        FROM users u
        INNER JOIN subscriptions s ON s.user_id = u.id AND s.status = 'active'
        WHERE u.is_active = true
          AND u.email IS NOT NULL
          AND u.email != ''
        ORDER BY u.id, s.created_at DESC
    """)
    rows = cursor.fetchall()
    subscribers = []
    for row in rows:
        user = dict(row)
        user['_states'] = parse_json_list(user.get('selected_states'))
        user['_areas'] = parse_json_list(user.get('selected_areas'))
        subscribers.append(user)
    return subscribers


def _already_notified(cursor, user_id, tender_id):
    cursor.execute(
        "SELECT 1 FROM tender_email_notifications WHERE user_id = %s AND tender_id = %s",
        (user_id, tender_id),
    )
    return cursor.fetchone() is not None


def _mark_notified(cursor, user_id, tender_id):
    cursor.execute(
        """
        INSERT INTO tender_email_notifications (user_id, tender_id)
        VALUES (%s, %s)
        ON CONFLICT (user_id, tender_id) DO NOTHING
        """,
        (user_id, tender_id),
    )


def _send_webhook(url_webhook, usuario, licitacao):
    try:
        payload = {
            'evento': 'nova_licitacao_relevante',
            'timestamp': datetime.now().isoformat(),
            'usuario': {
                'id': usuario['id'],
                'nome': usuario.get('full_name', ''),
                'email': usuario.get('email', ''),
            },
            'licitacao': {
                'id': licitacao.get('id'),
                'titulo': licitacao.get('titulo') or licitacao.get('title', ''),
                'objeto': licitacao.get('objeto') or licitacao.get('description', ''),
                'orgao': licitacao.get('orgao') or licitacao.get('organization_name', ''),
                'municipio': licitacao.get('municipio') or licitacao.get('municipality_name', ''),
                'estado': licitacao.get('estado') or licitacao.get('state_code', ''),
                'modalidade': licitacao.get('modalidade') or licitacao.get('modality', ''),
                'valor_estimado': licitacao.get('valor_estimado') or licitacao.get('estimated_value'),
                'valor_formatado': _format_currency(
                    licitacao.get('valor_estimado') or licitacao.get('estimated_value')
                ),
                'data_publicacao': str(
                    licitacao.get('data_publicacao') or licitacao.get('publication_date', '')
                ),
                'url_pncp': licitacao.get('url_pncp') or licitacao.get('detail_url', ''),
            },
        }
        response = http_requests.post(
            url_webhook,
            json=payload,
            timeout=10,
            headers={'Content-Type': 'application/json'},
        )
        if response.status_code in (200, 201, 202):
            logger.info(f"Webhook Zaia enviado para usuário ID {usuario['id']}")
        else:
            logger.warning(
                f"Webhook Zaia retornou {response.status_code} para usuário ID {usuario['id']}"
            )
    except Exception as exc:
        logger.error(f"Erro ao enviar webhook Zaia: {exc}")


def _build_pending_by_user(valid, subscribers, cursor):
    """Agrupa licitações pendentes por usuário (exclui já notificados)."""
    pending = {}
    skipped = 0

    for licitacao in valid:
        tender_id = licitacao['id']
        for usuario in subscribers:
            if not tender_matches_plan(licitacao, usuario['_states'], usuario['_areas']):
                continue

            user_id = usuario['id']
            if _already_notified(cursor, user_id, tender_id):
                skipped += 1
                continue

            if user_id not in pending:
                pending[user_id] = {'usuario': usuario, 'tenders': []}
            pending[user_id]['tenders'].append(licitacao)

    return pending, skipped


def _process_batch(licitacoes_list):
    """Envia 1 email resumo por usuário + webhooks individuais (se configurados)."""
    from src.services.email_service import send_tenders_digest

    valid = [l for l in licitacoes_list if l.get('id')]
    if not valid:
        logger.warning("Lote de notificações vazio ou sem IDs")
        return {'emails_sent': 0, 'emails_failed': 0, 'skipped': 0, 'tenders_notified': 0}

    conn = get_db_connection()
    if not conn:
        return {'emails_sent': 0, 'emails_failed': 0, 'skipped': 0, 'error': 'db_connection'}

    emails_sent = 0
    emails_failed = 0
    tenders_notified = 0
    cursor = None

    try:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        _ensure_notification_table(cursor)
        conn.commit()

        subscribers = _load_active_subscribers(cursor)
        if not subscribers:
            logger.info("Nenhum assinante ativo para notificar")
            return {'emails_sent': 0, 'emails_failed': 0, 'skipped': 0, 'tenders_notified': 0}

        pending_by_user, skipped = _build_pending_by_user(valid, subscribers, cursor)

        if not pending_by_user:
            logger.info(
                f"Nenhuma notificação pendente "
                f"({skipped} licitação(ões) já notificadas anteriormente)"
            )
            return {
                'emails_sent': 0,
                'emails_failed': 0,
                'skipped': skipped,
                'tenders_notified': 0,
            }

        logger.info(
            f"Enviando resumos: {len(pending_by_user)} usuário(s), "
            f"{sum(len(p['tenders']) for p in pending_by_user.values())} licitação(ões) no total"
        )

        for user_id, data in pending_by_user.items():
            usuario = data['usuario']
            tenders = data['tenders']
            email = usuario.get('email')
            if not email or not tenders:
                continue

            ok = send_tenders_digest(
                user_email=email,
                user_name=usuario.get('full_name') or 'Cliente',
                licitacoes=tenders,
            )

            if ok:
                for licitacao in tenders:
                    _mark_notified(cursor, user_id, licitacao['id'])
                conn.commit()
                emails_sent += 1
                tenders_notified += len(tenders)
                logger.info(
                    f"Resumo enviado para {email}: {len(tenders)} licitação(ões)"
                )
            else:
                conn.rollback()
                emails_failed += 1
                logger.warning(f"Falha ao enviar resumo para {email}")

            time.sleep(EMAIL_RATE_DELAY)

            webhook_url = (usuario.get('zaia_webhook_url') or '').strip()
            if webhook_url and ok:
                for licitacao in tenders:
                    _send_webhook(webhook_url, usuario, licitacao)

        logger.info(
            f"Lote concluído: {emails_sent} resumo(s) enviado(s) "
            f"({tenders_notified} licitação(ões)), "
            f"{emails_failed} falha(s), {skipped} ignorado(s) (já notificados)"
        )
        return {
            'emails_sent': emails_sent,
            'emails_failed': emails_failed,
            'skipped': skipped,
            'tenders_notified': tenders_notified,
        }

    except Exception as exc:
        logger.error(f"Erro ao processar lote de notificações: {exc}")
        try:
            conn.rollback()
        except Exception:
            pass
        return {
            'emails_sent': emails_sent,
            'emails_failed': emails_failed,
            'tenders_notified': tenders_notified,
            'error': str(exc),
        }

    finally:
        if cursor:
            try:
                cursor.close()
            except Exception:
                pass
        conn.close()


def notify_users_of_new_tenders_batch(licitacoes_list, wait=False):
    """
    Notifica assinantes sobre um lote de licitações novas (email resumo).
    wait=True: processa de forma síncrona (recomendado na automação).
    """
    if os.getenv('ENABLE_TENDER_EMAIL_NOTIFICATIONS', 'true').lower() not in ('1', 'true', 'yes'):
        logger.info("Notificações de licitação desabilitadas (ENABLE_TENDER_EMAIL_NOTIFICATIONS)")
        return

    if not licitacoes_list:
        return

    if wait:
        return _process_batch(licitacoes_list)

    thread = threading.Thread(
        target=_process_batch,
        args=(licitacoes_list,),
        daemon=True,
    )
    thread.start()
    logger.info(f"Notificações em lote agendadas para {len(licitacoes_list)} licitação(ões)")


def notify_users_of_new_tender(licitacao_dict, wait=False):
    """Notifica assinantes sobre uma única licitação nova (resumo com 1 item)."""
    notify_users_of_new_tenders_batch([licitacao_dict], wait=wait)


def disparar_webhooks_nova_licitacao(licitacao_dict):
    """Alias retrocompatível usado pela integração Zaia."""
    notify_users_of_new_tender(licitacao_dict, wait=False)
