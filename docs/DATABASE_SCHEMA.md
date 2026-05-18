# Database Schema

O banco de dados é o núcleo do sistema, armazenando informações de usuários, assinaturas e licitações.

## Banco Usado
PostgreSQL.

## Tabelas Principais
As tabelas identificadas incluem `users`, `subscriptions` e `tenders`.

## Campos Principais
- **tenders**: `id`, `pncp_id`, `title`, `objeto`, `description`, `organization_name`, `organization_cnpj`, `municipality_name`, `state_code`, `publication_date`, `status`, `modality`, `estimated_value`, `valor_total_estimado`, `detail_url`, `source_url`, `items_count`, `downloads_count`, `created_at`.
- **users**: `id`, `full_name`, `email`, `phone`, `company_name`, `user_type`, `password_hash`, `is_active`, `zaia_api_key`, `zaia_webhook_url`.
- **subscriptions**: `user_id`, `plan_name`, `status`, `selected_states`, `selected_areas`, `created_at`.

## Relacionamentos
A tabela `subscriptions` possui uma relação com a tabela `users` através do campo `user_id`.

## Regras Importantes
A tabela de licitações chama-se `tenders`, não `licitacoes`. O status padrão para busca é 'Publicado'.

## Migrations Existentes
INFORMAÇÃO NÃO CONFIRMADA.
