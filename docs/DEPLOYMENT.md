# Deployment

## Backend Web (Flask + Gunicorn)

- **Hospedagem:** Railway
- **URL produção:** `https://web-production-684c4.up.railway.app`
- **Start command:** `gunicorn --bind 0.0.0.0:$PORT src.main:app` (ver `Procfile`)

### Variáveis de ambiente obrigatórias

| Variável | Descrição |
|---|---|
| `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD` | PostgreSQL |
| `SECRET_KEY` | Flask |
| `RESEND_API_KEY`, `MAIL_FROM`, `FRONTEND_URL` | E-mails |
| Credenciais Mercado Pago | Pagamentos |

## Automação PNCP (Cron Railway)

A automação roda em um **serviço separado** do web server.

### 1. Criar serviço cron na Railway

1. No projeto Railway, clique em **New Service** → mesmo repositório do backend
2. Em **Settings → Config file path**, defina: `railway.automation.toml`
3. Copie as variáveis de ambiente do serviço web (principalmente `DB_*`)
4. Adicione as variáveis do scraper (abaixo)

### 2. Variáveis do scraper

| Variável | Padrão | Descrição |
|---|---|---|
| `SCRAPER_STATES` | `SP,RJ,MG,RS,PR,SC,BA,GO,DF` | UFs separadas por vírgula. Use `ALL` para os 27 estados |
| `SCRAPER_LIMIT_PER_STATE` | `10` | Máximo de editais coletados por UF |
| `SCRAPER_HEADLESS` | `true` | Browser sem interface gráfica |
| `SCRAPER_TIMEOUT_SECONDS` | `7200` | Timeout total do scraper (2h) |
| `SCRAPER_SKIP_SQLITE` | `true` | Ignora SQLite local em produção |

### 3. Horários (cron)

Definidos em `railway.automation.toml`:

- **09:00 UTC** → 06:00 BRT
- **15:00 UTC** → 12:00 BRT
- **21:00 UTC** → 18:00 BRT

### 4. Build com Playwright

O `nixpacks.toml` instala Chromium automaticamente:

```bash
pip install -r requirements.txt
python -m playwright install --with-deps chromium
```

### 5. Execução manual (local)

```bash
cd seglicit-backend
pip install -r requirements.txt
python -m playwright install chromium
python automacao_licitacoes_FINAL.py
```

Testar um estado apenas:

```bash
SCRAPER_STATES=SP SCRAPER_LIMIT_PER_STATE=3 python automacao_licitacoes_FINAL.py
```

### 6. Monitoramento

- Tabela `automation_logs` no PostgreSQL (status, novas licitações, tempo)
- Endpoint `GET /api/licitacoes/stats` expõe última execução
- Logs em `automacao_log_YYYYMMDD_HHMMSS.txt`

### 7. Escalar para todos os estados

Quando o cron estiver estável, altere na Railway:

```
SCRAPER_STATES=ALL
SCRAPER_LIMIT_PER_STATE=5
```
