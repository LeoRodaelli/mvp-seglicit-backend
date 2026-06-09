# -*- coding: utf-8 -*-
import os
import logging
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
        else:
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
