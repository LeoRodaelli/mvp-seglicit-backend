# -*- coding: utf-8 -*-
"""
Extração de município/UF do texto bruto dos cards do PNCP (formato
"Cidade/UF" ou "Cidade - UF", atrás de um rótulo como "Localidade da
Unidade:" ou "Local:").

Usado tanto pelo scraper (pncp_scraper_items_only.py) quanto pelo backfill
de correção de state_code (src/routes/tender.py) — uma única fonte de
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

# Ordem importa: o layout atual do PNCP usa "Localidade da Unidade:" — vem
# primeiro. "Local:" fica como fallback pra layouts antigos/variantes.
ROTULOS_LOCALIDADE = [
    'Localidade da Unidade:',
    'Local:',
]


def _parse_cidade_uf(local_part):
    """Recebe o texto já isolado depois do rótulo e separa cidade/UF."""
    local_part = (local_part or '').strip()
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


def extract_local_municipio_uf(text):
    """
    Extrai (municipio, uf) do texto do card do PNCP, tentando os rótulos
    conhecidos em ordem.

    uf vem None quando não for possível validar com segurança — o chamador
    deve tratar isso como "estado desconhecido" (não assumir nenhum
    default), não como erro silencioso.
    """
    if not text:
        return None, None

    for rotulo in ROTULOS_LOCALIDADE:
        if rotulo not in text:
            continue
        try:
            local_part = text.split(rotulo, 1)[1].split('\n', 1)[0]
        except Exception:
            continue
        municipio, uf = _parse_cidade_uf(local_part)
        if uf:
            return municipio, uf
        # Rótulo encontrado mas não deu pra validar a UF — tenta o
        # próximo rótulo antes de desistir (pode haver mais de um campo
        # de localidade no mesmo card).
        if municipio and not uf:
            fallback = (municipio, uf)
            continue

    try:
        return fallback
    except NameError:
        return None, None
