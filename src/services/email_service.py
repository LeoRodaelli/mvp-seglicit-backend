# -*- coding: utf-8 -*-
import html as html_module
import os
import logging
import time
import requests

logger = logging.getLogger(__name__)

RESEND_API_URL = 'https://api.resend.com/emails'


def send_email(to_email, subject, html_body):
    api_key = os.getenv('RESEND_API_KEY')
    mail_from = os.getenv('MAIL_FROM', 'Seglicit <noreply@seglicit.com.br>')

    if not api_key:
        logger.warning("⚠️ RESEND_API_KEY não configurado — email não enviado")
        return False

    logger.info(f"📧 Enviando email para {to_email} via Resend...")
    try:
        resp = requests.post(
            RESEND_API_URL,
            headers={
                'Authorization': f'Bearer {api_key}',
                'Content-Type': 'application/json'
            },
            json={
                'from': mail_from,
                'to': [to_email],
                'subject': subject,
                'html': html_body
            },
            timeout=10
        )

        if resp.status_code in (200, 201):
            logger.info(f"✅ Email enviado para {to_email}: {subject}")
            return True

        if resp.status_code == 429:
            logger.warning(f"⚠️ Resend rate limit — aguardando 1s e tentando novamente...")
            time.sleep(1)
            retry = requests.post(
                RESEND_API_URL,
                headers={
                    'Authorization': f'Bearer {api_key}',
                    'Content-Type': 'application/json'
                },
                json={
                    'from': mail_from,
                    'to': [to_email],
                    'subject': subject,
                    'html': html_body
                },
                timeout=10
            )
            if retry.status_code in (200, 201):
                logger.info(f"✅ Email enviado (retry) para {to_email}: {subject}")
                return True
            logger.error(f"❌ Resend retry retornou {retry.status_code}: {retry.text}")
            return False

        logger.error(f"❌ Resend retornou {resp.status_code}: {resp.text}")
        return False

    except Exception as e:
        logger.error(f"❌ Erro ao enviar email para {to_email}: {e}")
        return False


def send_payment_confirmation(customer_email, customer_name, plan_name, selected_states, selected_areas):
    frontend_url = os.getenv('FRONTEND_URL', 'https://seglicit.com.br')
    states_text = ", ".join(selected_states) if selected_states else "Todos"
    areas_text = ", ".join(selected_areas) if selected_areas else "Todas"

    html = f"""<!DOCTYPE html>
<html lang="pt-BR">
<body style="margin:0;padding:0;background:#f4f4f4;font-family:Arial,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0">
    <tr><td align="center" style="padding:30px 0;">
      <table width="600" cellpadding="0" cellspacing="0" style="background:#fff;border-radius:8px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,.1);">
        <tr>
          <td style="background:#1e3a5f;padding:24px;text-align:center;">
            <h1 style="color:#fff;margin:0;font-size:26px;">Seglicit</h1>
            <p style="color:#a8c4e0;margin:6px 0 0;">Plataforma de Licitações</p>
          </td>
        </tr>
        <tr>
          <td style="padding:32px;">
            <h2 style="color:#1e3a5f;margin:0 0 16px;">Pagamento confirmado! 🎉</h2>
            <p style="color:#444;">Olá, <strong>{customer_name}</strong>!</p>
            <p style="color:#444;">Seu plano foi ativado com sucesso. Confira os detalhes:</p>
            <table width="100%" cellpadding="0" cellspacing="0" style="background:#f0f5ff;border-left:4px solid #1e3a5f;border-radius:4px;margin:20px 0;">
              <tr><td style="padding:16px;">
                <p style="margin:0 0 8px;color:#333;"><strong>Plano:</strong> {plan_name}</p>
                <p style="margin:0 0 8px;color:#333;"><strong>Estados monitorados:</strong> {states_text}</p>
                <p style="margin:0;color:#333;"><strong>Áreas monitoradas:</strong> {areas_text}</p>
              </td></tr>
            </table>
            <p style="color:#444;">Acesse a plataforma com o <strong>email e senha</strong> cadastrados no momento da compra:</p>
            <table width="100%" cellpadding="0" cellspacing="0">
              <tr><td align="center" style="padding:20px 0;">
                <a href="{frontend_url}/login"
                   style="background:#1e3a5f;color:#fff;padding:14px 36px;border-radius:6px;text-decoration:none;font-size:16px;font-weight:bold;">
                  Acessar plataforma
                </a>
              </td></tr>
            </table>
            <p style="color:#888;font-size:13px;">Em caso de dúvidas, responda este email.</p>
          </td>
        </tr>
        <tr>
          <td style="background:#f9f9f9;padding:16px;text-align:center;border-top:1px solid #eee;">
            <p style="margin:0;color:#aaa;font-size:12px;">© 2026 Seglicit. Todos os direitos reservados.</p>
          </td>
        </tr>
      </table>
    </td></tr>
  </table>
</body>
</html>"""

    return send_email(
        customer_email,
        f"Bem-vindo ao Seglicit! Seu plano {plan_name} está ativo",
        html
    )


def send_password_reset(user_email, user_name, reset_token):
    frontend_url = os.getenv('FRONTEND_URL', 'https://seglicit.com.br')
    reset_link = f"{frontend_url}/reset-password?token={reset_token}"

    html = f"""<!DOCTYPE html>
<html lang="pt-BR">
<body style="margin:0;padding:0;background:#f4f4f4;font-family:Arial,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0">
    <tr><td align="center" style="padding:30px 0;">
      <table width="600" cellpadding="0" cellspacing="0" style="background:#fff;border-radius:8px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,.1);">
        <tr>
          <td style="background:#1e3a5f;padding:24px;text-align:center;">
            <h1 style="color:#fff;margin:0;font-size:26px;">Seglicit</h1>
            <p style="color:#a8c4e0;margin:6px 0 0;">Plataforma de Licitações</p>
          </td>
        </tr>
        <tr>
          <td style="padding:32px;">
            <h2 style="color:#1e3a5f;margin:0 0 16px;">Redefinição de senha</h2>
            <p style="color:#444;">Olá, <strong>{user_name}</strong>!</p>
            <p style="color:#444;">Recebemos uma solicitação para redefinir a senha da sua conta. Clique no botão abaixo para criar uma nova senha:</p>
            <table width="100%" cellpadding="0" cellspacing="0">
              <tr><td align="center" style="padding:24px 0;">
                <a href="{reset_link}"
                   style="background:#1e3a5f;color:#fff;padding:14px 36px;border-radius:6px;text-decoration:none;font-size:16px;font-weight:bold;">
                  Redefinir senha
                </a>
              </td></tr>
            </table>
            <p style="color:#666;font-size:14px;"><strong>Este link expira em 1 hora.</strong></p>
            <p style="color:#888;font-size:13px;">Se você não solicitou a redefinição de senha, ignore este email. Sua senha permanecerá a mesma.</p>
          </td>
        </tr>
        <tr>
          <td style="background:#f9f9f9;padding:16px;text-align:center;border-top:1px solid #eee;">
            <p style="margin:0;color:#aaa;font-size:12px;">© 2026 Seglicit. Todos os direitos reservados.</p>
          </td>
        </tr>
      </table>
    </td></tr>
  </table>
</body>
</html>"""

    return send_email(
        user_email,
        "Redefinição de senha - Seglicit",
        html
    )


def send_new_tender_alert(user_email, user_name, licitacao):
    """Email de alerta quando uma licitação nova corresponde ao plano do assinante."""
    frontend_url = os.getenv('FRONTEND_URL', 'https://seglicit.com.br')

    titulo = licitacao.get('title') or licitacao.get('titulo') or 'Sem título'
    objeto = licitacao.get('objeto') or licitacao.get('description') or ''
    orgao = licitacao.get('organization_name') or licitacao.get('orgao') or 'Não informado'
    municipio = licitacao.get('municipality_name') or licitacao.get('municipio') or ''
    estado = licitacao.get('state_code') or licitacao.get('estado') or ''
    modalidade = licitacao.get('modality') or licitacao.get('modalidade') or 'Não informada'
    valor = licitacao.get('estimated_value') or licitacao.get('valor_estimado')
    detail_url = licitacao.get('detail_url') or licitacao.get('url_pncp') or frontend_url
    pub_date = licitacao.get('publication_date') or licitacao.get('data_publicacao') or ''

    if valor:
        try:
            valor_fmt = f"R$ {float(valor):,.2f}".replace(',', 'TEMP').replace('.', ',').replace('TEMP', '.')
        except Exception:
            valor_fmt = 'Valor não informado'
    else:
        valor_fmt = 'Valor não informado'

    local = f"{municipio}/{estado}" if municipio and estado else (estado or municipio or 'Não informado')
    objeto_preview = (objeto[:280] + '...') if len(objeto) > 280 else objeto

    html = f"""<!DOCTYPE html>
<html lang="pt-BR">
<body style="margin:0;padding:0;background:#f4f4f4;font-family:Arial,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0">
    <tr><td align="center" style="padding:30px 0;">
      <table width="600" cellpadding="0" cellspacing="0" style="background:#fff;border-radius:8px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,.1);">
        <tr>
          <td style="background:#1e3a5f;padding:24px;text-align:center;">
            <h1 style="color:#fff;margin:0;font-size:26px;">Seglicit</h1>
            <p style="color:#a8c4e0;margin:6px 0 0;">Nova licitação para o seu plano</p>
          </td>
        </tr>
        <tr>
          <td style="padding:32px;">
            <h2 style="color:#1e3a5f;margin:0 0 16px;">Nova oportunidade encontrada</h2>
            <p style="color:#444;">Olá, <strong>{user_name}</strong>!</p>
            <p style="color:#444;">Encontramos uma licitação que corresponde aos <strong>estados e áreas</strong> do seu plano:</p>
            <table width="100%" cellpadding="0" cellspacing="0" style="background:#f0f5ff;border-left:4px solid #1e3a5f;border-radius:4px;margin:20px 0;">
              <tr><td style="padding:16px;">
                <p style="margin:0 0 10px;color:#333;font-size:16px;font-weight:bold;">{titulo}</p>
                <p style="margin:0 0 8px;color:#555;font-size:14px;">{objeto_preview}</p>
                <p style="margin:0 0 6px;color:#333;"><strong>Órgão:</strong> {orgao}</p>
                <p style="margin:0 0 6px;color:#333;"><strong>Local:</strong> {local}</p>
                <p style="margin:0 0 6px;color:#333;"><strong>Modalidade:</strong> {modalidade}</p>
                <p style="margin:0 0 6px;color:#333;"><strong>Valor estimado:</strong> {valor_fmt}</p>
                <p style="margin:0;color:#333;"><strong>Publicação:</strong> {pub_date}</p>
              </td></tr>
            </table>
            <table width="100%" cellpadding="0" cellspacing="0">
              <tr><td align="center" style="padding:8px 0 20px;">
                <a href="{detail_url}"
                   style="background:#1e3a5f;color:#fff;padding:14px 36px;border-radius:6px;text-decoration:none;font-size:16px;font-weight:bold;">
                  Ver licitação no PNCP
                </a>
              </td></tr>
              <tr><td align="center">
                <a href="{frontend_url}/login"
                   style="color:#1e3a5f;font-size:14px;text-decoration:underline;">
                  Acessar plataforma Seglicit
                </a>
              </td></tr>
            </table>
            <p style="color:#888;font-size:13px;">Você recebe este email porque possui um plano ativo na Seglicit.</p>
          </td>
        </tr>
        <tr>
          <td style="background:#f9f9f9;padding:16px;text-align:center;border-top:1px solid #eee;">
            <p style="margin:0;color:#aaa;font-size:12px;">© 2026 Seglicit. Todos os direitos reservados.</p>
          </td>
        </tr>
      </table>
    </td></tr>
  </table>
</body>
</html>"""

    estado_label = estado or 'BR'
    return send_email(
        user_email,
        f"[Seglicit] Nova licitação em {estado_label}: {titulo[:60]}",
        html,
    )


def _format_valor_brl(valor):
    if not valor:
        return 'Valor não informado'
    try:
        return f"R$ {float(valor):,.2f}".replace(',', 'TEMP').replace('.', ',').replace('TEMP', '.')
    except Exception:
        return 'Valor não informado'


def send_tenders_digest(user_email, user_name, licitacoes):
    """
    Email resumo com várias licitações novas para o plano do assinante.
    Um único email por lote/automação em vez de um email por licitação.
    """
    frontend_url = os.getenv('FRONTEND_URL', 'https://seglicit.com.br')
    max_items = int(os.getenv('DIGEST_MAX_ITEMS_IN_EMAIL', '15'))
    total = len(licitacoes)
    shown = licitacoes[:max_items]
    restante = total - len(shown)

    items_html = ''
    for index, licitacao in enumerate(shown, 1):
        titulo = html_module.escape(licitacao.get('title') or licitacao.get('titulo') or 'Sem título')
        orgao = html_module.escape(
            licitacao.get('organization_name') or licitacao.get('orgao') or 'Não informado'
        )
        municipio = licitacao.get('municipality_name') or licitacao.get('municipio') or ''
        estado = licitacao.get('state_code') or licitacao.get('estado') or ''
        local = f"{municipio}/{estado}" if municipio and estado else (estado or municipio or 'Não informado')
        local = html_module.escape(local)
        modalidade = html_module.escape(
            licitacao.get('modality') or licitacao.get('modalidade') or 'Não informada'
        )
        valor_fmt = _format_valor_brl(
            licitacao.get('estimated_value') or licitacao.get('valor_estimado')
        )
        detail_url = html_module.escape(
            licitacao.get('detail_url') or licitacao.get('url_pncp') or frontend_url
        )
        objeto = licitacao.get('objeto') or licitacao.get('description') or ''
        objeto_preview = html_module.escape((objeto[:120] + '...') if len(objeto) > 120 else objeto)

        items_html += f"""
            <table width="100%" cellpadding="0" cellspacing="0"
                   style="background:#f8faff;border-left:4px solid #1e3a5f;border-radius:4px;margin:0 0 16px;">
              <tr><td style="padding:16px;">
                <p style="margin:0 0 6px;color:#888;font-size:12px;font-weight:bold;">#{index}</p>
                <p style="margin:0 0 8px;color:#1e3a5f;font-size:15px;font-weight:bold;">{titulo}</p>
                <p style="margin:0 0 8px;color:#555;font-size:13px;">{objeto_preview}</p>
                <p style="margin:0 0 4px;color:#333;font-size:13px;"><strong>Órgão:</strong> {orgao}</p>
                <p style="margin:0 0 4px;color:#333;font-size:13px;"><strong>Local:</strong> {local}</p>
                <p style="margin:0 0 4px;color:#333;font-size:13px;"><strong>Modalidade:</strong> {modalidade}</p>
                <p style="margin:0 0 10px;color:#333;font-size:13px;"><strong>Valor:</strong> {valor_fmt}</p>
                <a href="{detail_url}"
                   style="color:#1e3a5f;font-size:13px;font-weight:bold;text-decoration:underline;">
                  Ver no PNCP →
                </a>
              </td></tr>
            </table>"""

    restante_html = ''
    if restante > 0:
        restante_html = f"""
            <p style="color:#666;font-size:14px;text-align:center;margin:8px 0 20px;">
              <strong>+ {restante} licitação(ões)</strong> disponível(is) na plataforma.
            </p>"""

    if total == 1:
        titulo_email = '1 nova licitação para o seu plano'
        intro = 'Encontramos <strong>1 licitação</strong> que corresponde ao seu plano:'
    else:
        titulo_email = f'{total} novas licitações para o seu plano'
        intro = f'Encontramos <strong>{total} licitações</strong> que correspondem ao seu plano:'

    html = f"""<!DOCTYPE html>
<html lang="pt-BR">
<body style="margin:0;padding:0;background:#f4f4f4;font-family:Arial,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0">
    <tr><td align="center" style="padding:30px 0;">
      <table width="600" cellpadding="0" cellspacing="0" style="background:#fff;border-radius:8px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,.1);">
        <tr>
          <td style="background:#1e3a5f;padding:24px;text-align:center;">
            <h1 style="color:#fff;margin:0;font-size:26px;">Seglicit</h1>
            <p style="color:#a8c4e0;margin:6px 0 0;">Resumo de novas licitações</p>
          </td>
        </tr>
        <tr>
          <td style="padding:32px;">
            <h2 style="color:#1e3a5f;margin:0 0 16px;">{html_module.escape(titulo_email)}</h2>
            <p style="color:#444;">Olá, <strong>{html_module.escape(user_name)}</strong>!</p>
            <p style="color:#444;">{intro}</p>
            {items_html}
            {restante_html}
            <table width="100%" cellpadding="0" cellspacing="0">
              <tr><td align="center" style="padding:8px 0 20px;">
                <a href="{frontend_url}/login"
                   style="background:#1e3a5f;color:#fff;padding:14px 36px;border-radius:6px;text-decoration:none;font-size:16px;font-weight:bold;">
                  Ver todas na plataforma
                </a>
              </td></tr>
            </table>
            <p style="color:#888;font-size:13px;">Você recebe este resumo porque possui um plano ativo na Seglicit.</p>
          </td>
        </tr>
        <tr>
          <td style="background:#f9f9f9;padding:16px;text-align:center;border-top:1px solid #eee;">
            <p style="margin:0;color:#aaa;font-size:12px;">© 2026 Seglicit. Todos os direitos reservados.</p>
          </td>
        </tr>
      </table>
    </td></tr>
  </table>
</body>
</html>"""

    return send_email(
        user_email,
        f"[Seglicit] {titulo_email}",
        html,
    )
