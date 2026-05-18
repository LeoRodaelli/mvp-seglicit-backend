# Seglicit — Contexto Geral do Produto

## Nome do Projeto
Seglicit (mvp-licitacoes-melhorado)

## Objetivo do Projeto
O Seglicit é uma plataforma de monitoramento e busca de licitações públicas. O objetivo é facilitar o acesso a editais, contratos e oportunidades governamentais para empresas de diversos setores, permitindo que encontrem licitações relevantes para seus negócios de forma rápida e automatizada.

## Problema que Resolve
A busca manual por licitações em diários oficiais e portais governamentais (como o PNCP) é demorada, complexa e ineficiente. O Seglicit centraliza essas informações, aplica filtros inteligentes (por estado, área de atuação, valor, etc.) e notifica os usuários sobre novas oportunidades, economizando tempo e aumentando as chances de participação em certames.

## Tipo de Usuário
- **Visitantes/Leads:** Usuários não autenticados que acessam o site público em busca de informações sobre a plataforma e planos.
- **Clientes (Assinantes):** Empresas e profissionais que possuem um plano ativo (Individual ou Combo Completo) e utilizam a plataforma para buscar e monitorar licitações específicas de seus segmentos e estados de interesse.

## Fluxo Principal do Sistema
1. **Acesso Público:** O visitante acessa o site, conhece os planos e interage com o Agente IA de Vendas (Zaia) para tirar dúvidas.
2. **Assinatura:** O usuário escolhe um plano e realiza o pagamento (integração com Mercado Pago).
3. **Configuração de Preferências:** Após a assinatura, o usuário define seus estados e áreas de atuação de interesse.
4. **Busca e Monitoramento:** O usuário acessa a plataforma autenticada para buscar licitações usando filtros avançados ou interage com o Agente IA da Plataforma para consultas rápidas.
5. **Notificações:** O sistema monitora novas licitações e envia alertas (via webhook/Zaia) para os usuários cujas preferências correspondam às novas oportunidades.

## Funcionalidades Principais
- Busca avançada de licitações (por palavra-chave, estado, área, valor, data).
- Visualização detalhada de licitações (título, órgão, valor, arquivos, etc.).
- Favoritar licitações para acesso rápido.
- Visualizador de PDF integrado para leitura de editais.
- Agente IA de Vendas (público) para conversão de leads.
- Agente IA da Plataforma (autenticado) para busca interativa de licitações.
- Sistema de assinaturas com planos diferenciados (Individual e Combo Completo).
- Notificações automáticas de novas licitações relevantes.

## Como Frontend e Backend se Conectam
O frontend (React) se comunica com o backend (Flask/Python) através de uma API RESTful. As requisições são autenticadas (geralmente via tokens ou sessões, detalhes específicos na documentação do backend). O backend acessa o banco de dados PostgreSQL para recuperar e armazenar informações, e também se integra com serviços externos (como a API da Zaia para os agentes IA).

## Status Geral Atual
O MVP está funcional, com o backend hospedado no Railway e o frontend na Vercel. A integração com a Zaia para o agente da plataforma foi recentemente ajustada para corrigir problemas de alucinação e exibição de dados.

## O que está pronto
- Backend estruturado em Flask com banco de dados PostgreSQL.
- Frontend em React com interface de busca, detalhes e visualizador de PDF.
- Integração com a API da Zaia para os dois agentes (Vendas e Plataforma).
- Endpoint simplificado (`/api/zaia/buscar`) para o agente da plataforma.
- Dicionário de palavras-chave por área de atuação no backend.

## O que falta
- INFORMAÇÃO NÃO CONFIRMADA (O contexto atual foca na correção do agente Zaia, não há uma lista exaustiva de pendências gerais do projeto além de possíveis refinamentos na interface ou novas integrações).

## Pontos Críticos
- **Agentes IA (Zaia):** É crucial manter a separação entre o Agente de Vendas (público) e o Agente da Plataforma (autenticado) para evitar alucinações. O agente da plataforma deve usar o endpoint `/api/zaia/buscar` e o modelo GPT-4o Mini (ou similar que siga instruções estritas).
- **Dicionário de Keywords:** A busca por área depende do dicionário `KEYWORDS_POR_AREA` no backend. Qualquer alteração nas áreas suportadas exige atualização deste dicionário.

## Decisões Importantes Já Tomadas
- **Separação de Agentes IA:** Decidiu-se usar dois agentes Zaia distintos (ID 69739 para vendas, ID 76034 para a plataforma) para resolver problemas de contexto e alucinação.
- **Endpoint Simplificado para Zaia:** Criação do endpoint `/api/zaia/buscar` que retorna texto puro formatado para evitar que o LLM se confunda com JSONs complexos.
- **Dicionário de Keywords no Backend:** A expansão de áreas de atuação para palavras-chave específicas é feita no backend, garantindo buscas mais precisas sem depender da interpretação do LLM.

## O que a próxima IA precisa saber antes de mexer no projeto
- O projeto possui dois repositórios separados (Frontend e Backend).
- A integração com a Zaia é um ponto sensível. Leia o `SEGLICIT_AGENTE_ZAIA_FLUXO.md` e o `GUIA_INTEGRACAO_ZAIA_API.md` para entender a arquitetura dos agentes.
- O banco de dados usa a tabela `tenders` (não `licitacoes`) para armazenar as oportunidades.
- O frontend possui um botão flutuante para o chat da Zaia na plataforma autenticada.
