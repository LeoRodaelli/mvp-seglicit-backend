# -*- coding: utf-8 -*-
"""Validação de email para checkout (formato, domínio e MX)."""

import re
import logging

logger = logging.getLogger(__name__)

EMAIL_REGEX = re.compile(
    r'^[a-zA-Z0-9](?:[a-zA-Z0-9._%+-]{0,62}[a-zA-Z0-9])?'
    r'@'
    r'[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?'
    r'(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)+$'
)

DOMAIN_TYPOS = {
    'gmial.com': 'gmail.com',
    'gmal.com': 'gmail.com',
    'gamil.com': 'gmail.com',
    'gnail.com': 'gmail.com',
    'hotmial.com': 'hotmail.com',
    'hotmal.com': 'hotmail.com',
    'outlok.com': 'outlook.com',
    'outllok.com': 'outlook.com',
    'yaho.com': 'yahoo.com',
    'yahooo.com': 'yahoo.com',
}

DISPOSABLE_DOMAINS = {
    'mailinator.com',
    'guerrillamail.com',
    'tempmail.com',
    '10minutemail.com',
    'yopmail.com',
    'throwaway.email',
    'getnada.com',
    'maildrop.cc',
    'temp-mail.org',
}


def _domain_has_mx_or_a(domain):
    try:
        import dns.resolver
    except ImportError:
        logger.warning('dnspython não instalado — pulando verificação MX')
        return True, None

    resolver = dns.resolver.Resolver()
    resolver.lifetime = 4.0

    try:
        answers = resolver.resolve(domain, 'MX')
        if answers:
            return True, None
    except Exception:
        pass

    try:
        answers = resolver.resolve(domain, 'A')
        if answers:
            return True, None
    except Exception:
        pass

    try:
        answers = resolver.resolve(domain, 'AAAA')
        if answers:
            return True, None
    except Exception:
        pass

    return False, 'Domínio de email não encontrado. Verifique se o endereço está correto.'


def validate_checkout_email(email):
    """
    Valida email para checkout/assinatura MP.
    Retorna dict: { valid, message, suggestion }
    """
    raw = (email or '').strip()
    normalized = raw.lower()

    if not raw:
        return {'valid': False, 'message': 'Email é obrigatório', 'suggestion': None}

    if len(raw) > 254:
        return {'valid': False, 'message': 'Email muito longo', 'suggestion': None}

    if not EMAIL_REGEX.match(normalized):
        return {'valid': False, 'message': 'Formato de email inválido', 'suggestion': None}

    local, domain = normalized.split('@', 1)

    if domain in DISPOSABLE_DOMAINS:
        return {
            'valid': False,
            'message': 'Use um email permanente (não temporário)',
            'suggestion': None,
        }

    if domain in DOMAIN_TYPOS:
        suggested = f'{local}@{DOMAIN_TYPOS[domain]}'
        return {
            'valid': False,
            'message': f'Domínio incorreto. Você quis dizer {suggested}?',
            'suggestion': suggested,
        }

    ok, dns_message = _domain_has_mx_or_a(domain)
    if not ok:
        return {'valid': False, 'message': dns_message, 'suggestion': None}

    return {
        'valid': True,
        'message': 'Email válido — use o mesmo no Mercado Pago',
        'suggestion': None,
    }
