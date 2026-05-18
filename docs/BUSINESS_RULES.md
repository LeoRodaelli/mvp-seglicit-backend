# Business Rules

As regras de negócio garantem a entrega correta de informações aos usuários com base em seus planos.

## Regras de Negócio Implementadas
A filtragem de licitações por área de atuação não busca apenas pelo nome exato da área, mas utiliza um dicionário extenso de palavras-chave (`KEYWORDS_POR_AREA`) para encontrar licitações relacionadas.

## Validações
As buscas consideram as preferências do usuário (estados e áreas) definidas em sua assinatura (`subscription`), a menos que parâmetros explícitos sejam passados na requisição.

## Permissões
O acesso aos endpoints de busca e perfil requer uma chave de API válida associada a um usuário ativo.

## Casos Especiais
Para o agente de IA da plataforma, foi criado um endpoint específico (`/api/zaia/buscar`) que retorna os dados pré-formatados em texto, pois o modelo de IA apresentava dificuldades em processar estruturas JSON complexas.
