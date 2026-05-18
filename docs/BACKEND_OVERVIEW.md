# Backend Overview

A arquitetura do backend do Seglicit é construída para suportar a busca, filtragem e notificação de licitações públicas, além de gerenciar a integração com agentes de inteligência artificial.

## Stack Usada
O backend é desenvolvido em Python utilizando o framework Flask. O banco de dados relacional escolhido é o PostgreSQL.

## Estrutura Geral
A aplicação segue uma estrutura modular baseada em Blueprints do Flask, separando as rotas da API (como as integrações com a Zaia) da lógica de acesso a dados e utilitários.

## Principais Módulos
O módulo principal identificado é o `zaia_api.py`, que gerencia toda a comunicação com a plataforma Zaia AI. Este módulo inclui a geração de chaves de API, recuperação de perfis de usuários, busca complexa e simplificada de licitações, e configuração de webhooks.

## Serviços Externos
O sistema integra-se fortemente com a plataforma Zaia AI para fornecer agentes conversacionais. Também há menção a integrações de pagamento via Mercado Pago para o gerenciamento de assinaturas.

## Autenticação
A autenticação para os endpoints da API da Zaia é realizada através de chaves de API (`X-API-Key`). As chaves são geradas por usuário e validadas em cada requisição através de um decorador personalizado (`@require_api_key`).

## Integrações
A integração mais crítica é com a Zaia AI, utilizando webhooks para notificações ativas e endpoints REST para consultas passivas realizadas pelos agentes.
