# Architecture

A arquitetura do backend é projetada para ser leve e responsiva, focada em servir dados estruturados para o frontend e para os agentes de IA.

## Arquitetura do Backend
O sistema utiliza uma arquitetura monolítica modular baseada em Flask. A lógica de negócios está acoplada aos manipuladores de rotas, com acesso direto ao banco de dados via `psycopg2`.

## Organização de Pastas
INFORMAÇÃO NÃO CONFIRMADA. (A estrutura completa de pastas não está disponível no contexto, apenas o arquivo `zaia_api.py` que reside em `src/routes/`).

## Padrões Usados
O projeto utiliza o padrão Blueprint do Flask para roteamento. O acesso a dados é feito através de consultas SQL brutas usando cursores de dicionário (`RealDictCursor`) para facilitar a serialização JSON.

## Fluxo das Requisições
As requisições chegam aos endpoints do Flask, passam pelo middleware de autenticação (se aplicável), executam consultas SQL parametrizadas no PostgreSQL, formatam os resultados e retornam respostas JSON.
