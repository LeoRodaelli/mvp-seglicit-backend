# Claude Code Instructions (Backend)

Diretrizes para o Claude Code no backend.

- Utilize o padrão Blueprint do Flask para novas rotas.
- Mantenha as consultas SQL parametrizadas para evitar injeção de SQL.
- Ao lidar com a integração Zaia, lembre-se que o endpoint `/api/zaia/buscar` deve retornar texto puro formatado no campo `resultado`.
- A tabela principal de dados é `tenders`, não `licitacoes`.
