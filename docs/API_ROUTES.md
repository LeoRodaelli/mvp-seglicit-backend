# API Routes

A API fornece endpoints específicos para a integração com a Zaia AI.

## Endpoints Existentes

| Endpoint | Método | Payload Esperado | Resposta Esperada | Autenticação | Observações |
|---|---|---|---|---|---|
| `/api/zaia/ping` | GET | Nenhum | Status da API | Não | Endpoint público de verificação. |
| `/api/zaia/gerar-api-key` | POST | `{"user_id": 123, "password": "..."}` | API Key gerada | Não (usa credenciais no body) | Gera chave para uso nos outros endpoints. |
| `/api/zaia/perfil` | GET | Nenhum | Dados do usuário e plano | Sim (`X-API-Key`) | Retorna preferências da subscription. |
| `/api/zaia/licitacoes` | GET | Query params: `q`, `estados`, `areas`, `data_inicio`, etc. | Lista detalhada de licitações | Sim (`X-API-Key`) | Busca complexa com expansão de keywords. |
| `/api/zaia/licitacoes/<id>` | GET | Nenhum | Detalhes completos da licitação | Sim (`X-API-Key`) | Inclui itens e arquivos anexos. |
| `/api/zaia/configurar-webhook` | POST | `{"url_webhook": "..."}` | Confirmação | Sim (`X-API-Key`) | Salva URL para notificações ativas. |
| `/api/zaia/buscar` | GET | Query params: `q`, `estados`, `data_inicio` | Texto puro formatado no campo `resultado` | Sim (`X-API-Key`) | Endpoint simplificado para evitar alucinações do LLM. |
