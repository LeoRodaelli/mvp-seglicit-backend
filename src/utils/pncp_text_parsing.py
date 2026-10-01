# -*- coding: utf-8 -*-
"""
Extração de município/UF do texto bruto dos cards do PNCP (formato
"Local: Cidade/UF" ou "Local: Cidade - UF").

Usado tanto pelo scraper (pncp_scraper_items_only.py) quanto pelo backfill
de correção de state_code (src/routes/admin.py) — uma única fonte de
verdade pra essa extração, pra nunca mais divergir entre os dois.

IMPORTANTE: nunca "chuta" um estado default quando não consegue extrair
com segurança. Um state_code errado é pior que nenhum — faz a licitação
aparecer pro assinante errado, filtrada por um estado que não é o dela de
verdade (foi exatamente o bug que gerou esse módulo: o código antigo caía
num fallback fixo em "SP" sempre que a extração falhava).
"""
import re

UFS_VALIDAS = {
    'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MT', 'MS',
    'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 'RS', 'RO', 'RR', 'SC',
    'SP', 'SE', 'TO',
}


def extract_local_municipio_uf(text):
    """
    Extrai (municipio, uf) do texto do card do PNCP.

    uf vem None quando não for possível validar com segurança — o chamador
    deve tratar isso como "estado desconhecido" (não assumir nenhum
    default), não como erro silencioso.
    """
    if not text or 'Local:' not in text:
        return None, None

    try:
        local_part = text.split('Local:', 1)[1].split('\n', 1)[0].strip()
    except Exception:
        return None, None

    if not local_part:
        return None, None

    separator = None
    if '/' in local_part:
        separator = '/'
    elif ' - ' in local_part:
        separator = ' - '
    elif '-' in local_part:
        separator = '-'

    if not separator:
        # Sem separador reconhecível — devolve o texto bruto como
        # município, mas não arrisca um estado.
        return local_part, None

    municipio, _, uf_raw = local_part.rpartition(separator)
    municipio = municipio.strip() or None
    uf = re.sub(r'[^A-Za-z]', '', uf_raw or '').upper()

    if len(uf) == 2 and uf in UFS_VALIDAS:
        return municipio, uf

    # Separador encontrado mas o que veio depois não é uma UF válida
    # (ex: texto incompleto, cortado) — não arrisca.
    return local_part, None
