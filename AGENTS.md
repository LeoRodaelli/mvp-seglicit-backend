# Agents (Backend)

Instruções para agentes de IA operando no repositório do backend.

Ao trabalhar neste repositório, concentre-se na estabilidade e precisão da API. O backend é a fonte da verdade para as licitações.
Sempre verifique as consultas SQL no arquivo `zaia_api.py` para garantir que os nomes das colunas correspondam ao esquema da tabela `tenders`.
Ao modificar a lógica de busca, certifique-se de que o dicionário `KEYWORDS_POR_AREA` seja utilizado corretamente para expandir os termos de pesquisa.
