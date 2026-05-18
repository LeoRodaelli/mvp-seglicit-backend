> **AVISO DE SEGURANÇA:** chave real removida. Não salve API keys reais em documentação versionada. Use variável de ambiente ou gerenciador de secrets.

# Guia de Integração: Agente Zaia AI com API Seglicit

Este documento detalha o processo passo a passo para integrar o agente da Zaia AI com a API do Seglicit, permitindo que o chatbot consulte licitações em tempo real e responda aos usuários com base nos dados da plataforma.

A integração é feita utilizando o recurso de **Ação de Chamada de API (HTTP Request Action)** nativo da plataforma Zaia, sem necessidade de escrever código adicional no agente.

## 1. Preparação e Credenciais

Antes de iniciar a configuração na plataforma Zaia, certifique-se de ter as seguintes informações em mãos:

*   **URL Base da API:** `https://web-production-684c4.up.railway.app/api/zaia`
*   **API Key:** `ZAIA_API_KEY_REMOVIDA_USAR_VARIAVEL_DE_AMBIENTE` (Chave gerada para o usuário de teste)
*   **Endpoint de Busca:** `/licitacoes`

## 2. Configurando a Ação de Chamada de API na Zaia

Acesse o painel da Zaia (zaia.app) e navegue até as configurações do seu agente. O processo de integração ocorre dentro dos **Estágios do Agente**.

### Passo 2.1: Acessar o Estágio do Agente

1.  No menu lateral, selecione o seu agente.
2.  Vá para a seção de **Estágios** (Stages) ou **Fluxo de Conversa**.
3.  Selecione o estágio onde o agente deve realizar a busca de licitações (por exemplo, um estágio chamado "Busca de Licitações" ou o estágio principal de atendimento).

### Passo 2.2: Adicionar a Ação HTTP Request

1.  Dentro do estágio selecionado, procure pela opção de adicionar uma nova **Ação** (Action).
2.  Escolha o tipo **Chamada de API** (HTTP Request Action) [1].
3.  Preencha os campos da ação conforme detalhado abaixo:

**Configuração da Requisição:**

*   **Nome da Ação:** `Buscar Licitacoes Seglicit` (ou um nome descritivo de sua preferência).
*   **Método (Method):** `GET`
*   **URL (Endpoint):** `https://web-production-684c4.up.railway.app/api/zaia/licitacoes`

**Configuração de Headers:**

Adicione o seguinte cabeçalho de autenticação para que a API do Seglicit autorize a requisição:

| Header | Valor |
| :--- | :--- |
| `X-API-Key` | `ZAIA_API_KEY_REMOVIDA_USAR_VARIAVEL_DE_AMBIENTE` |

**Configuração de Parâmetros (Query Parameters):**

A API do Seglicit aceita parâmetros na URL para filtrar as licitações. Você deve configurar a Zaia para passar as intenções do usuário como parâmetros.

| Parâmetro | Descrição | Exemplo de Valor Dinâmico (Variável Zaia) |
| :--- | :--- | :--- |
| `estados` | Sigla do estado (ex: SP, RJ) | `{{estado_desejado}}` |
| `keywords` | Palavras-chave para busca | `{{termo_busca}}` |
| `date_from` | Data inicial (YYYY-MM-DD) | `{{data_inicio}}` |
| `valor_min` | Valor mínimo estimado | `{{valor_minimo}}` |

*Nota: A forma exata de inserir variáveis dinâmicas (`{{variavel}}`) depende da sintaxe específica da interface da Zaia para mapeamento de entidades extraídas da conversa.*

### Passo 2.3: Processamento da Resposta (Data Extraction)

Após configurar a requisição, você precisa instruir a Zaia sobre como ler a resposta JSON da API do Seglicit.

1.  Ative a opção de extração de dados (geralmente chamada de *Extract Data of Interest* ou similar) [1].
2.  A API do Seglicit retorna um JSON com a lista de licitações no campo `tenders`.
3.  Configure o agente para ler a lista de `tenders` e formatar a resposta para o usuário, extraindo campos como `title`, `objeto`, `organization_name`, `municipality_name`, `state_code` e `estimated_value_formatted`.

## 3. Instruções do Agente (Prompt / System Message)

Para que o agente saiba *quando* acionar a API e *como* apresentar os resultados, você deve atualizar as instruções base (Cérebro/Prompt) do agente.

Adicione diretrizes semelhantes a estas:

> "Você é um assistente especializado em licitações públicas da plataforma Seglicit.
> Quando o usuário solicitar a busca de licitações, você deve utilizar a ação 'Buscar Licitacoes Seglicit'.
> Antes de chamar a ação, certifique-se de extrair os filtros desejados pelo usuário, como estado (ex: SP, RJ), palavras-chave (ex: medicamento, tecnologia) e datas.
> Ao receber os resultados da API, apresente as licitações de forma clara e resumida, incluindo o título, órgão, local e valor estimado.
> Se a API não retornar resultados, informe educadamente que não foram encontradas licitações com os critérios informados e sugira ampliar a busca."

## 4. Teste e Validação

Antes de disponibilizar o agente para os clientes, utilize o simulador de chat interno da plataforma Zaia para testar a integração.

1.  Inicie uma conversa de teste.
2.  Peça: "Busque licitações de tecnologia em São Paulo".
3.  Verifique nos logs de execução da Zaia se a requisição HTTP foi disparada corretamente para a URL do Seglicit, com os parâmetros `estados=SP` e `keywords=tecnologia`.
4.  Confirme se o agente respondeu com os dados reais retornados pela API.

## Referências

[1] Zaia Docs. "[POST] Criar Ação de Chamada API". Disponível em: https://zaiadocs.gitbook.io/recursos/api/acoes-do-estagio/acao-de-chamada-de-api/post-criar-acao-de-chamada-api. Acesso em: 01 abr. 2026.
