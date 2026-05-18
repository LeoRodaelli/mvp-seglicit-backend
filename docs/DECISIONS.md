# Decisions

Decisões arquiteturais e técnicas tomadas durante o desenvolvimento.

## Decisões Técnicas Já Tomadas
Foi decidido criar um endpoint de busca simplificado (`/api/zaia/buscar`) que retorna uma string formatada em vez de um JSON estruturado.

## Motivo das Decisões
Modelos de linguagem (LLMs) menores ou mais rápidos (como o Gemini Flash Lite) apresentavam alucinações ou falhavam ao tentar formatar respostas JSON complexas contendo arrays de licitações. A formatação no backend garante a precisão dos dados apresentados.

## O Que Não Deve Ser Alterado Sem Cuidado
O dicionário `KEYWORDS_POR_AREA` e a lógica de expansão de busca. Alterações aqui afetam diretamente a precisão dos resultados entregues aos usuários e aos agentes de IA.
