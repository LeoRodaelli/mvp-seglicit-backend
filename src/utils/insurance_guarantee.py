"""
Detecção de exigência de seguro-garantia (garantia contratual na modalidade
"seguro-garantia", art. 96 da Lei 14.133/2021) no texto já armazenado da
licitação (objeto, description, detailed_description).

Não há parsing do PDF do edital em produção (o pipeline de PDF existente em
src/services/pdf_analyzer.py está ligado a um modelo legado `editais`,
desconectado da tabela `tenders`) — então esta é uma checagem por
palavra-chave sobre o texto/resumo já salvo no banco (vindo do PNCP).
Isso tem recall imperfeito: só pega o caso em que a exigência já aparece
no resumo/objeto salvo, não quando ela só está dentro do PDF anexado.
"""

import re

# Casa "seguro garantia", "seguro-garantia", "seguro de garantia" etc.
# Propositalmente NÃO casa só "garantia" sozinho (aparece em quase todo
# contrato só por causa do percentual de garantia contratual, e geraria
# falso positivo em praticamente toda licitação).
_SEGURO_GARANTIA_PATTERN = re.compile(r'seguro[\s-]*(?:de[\s-]*)?garantia', re.IGNORECASE)


def requires_seguro_garantia(*texts) -> bool:
    """Retorna True se algum dos textos passados menciona seguro-garantia."""
    for text in texts:
        if text and _SEGURO_GARANTIA_PATTERN.search(text):
            return True
    return False


def seguro_garantia_sql_clause(table_alias: str = '') -> str:
    """SQL: mesma checagem, para usar em WHERE (filtro opcional na busca)."""
    prefix = f'{table_alias}.' if table_alias else ''
    return f"""(
        {prefix}objeto ~* 'seguro[[:space:]-]*(de[[:space:]-]*)?garantia'
        OR {prefix}description ~* 'seguro[[:space:]-]*(de[[:space:]-]*)?garantia'
        OR {prefix}detailed_description ~* 'seguro[[:space:]-]*(de[[:space:]-]*)?garantia'
    )"""
