#!/usr/bin/env python3
"""
Script de Automação Completa - Licitações em Tempo Real (VERSÃO FINAL)

Fases:
  1. Scraper PNCP (Playwright) — multi-estado via SCRAPER_STATES
  2. Carga JSON → PostgreSQL (tenders)
  3. Limpeza de arquivos temporários + log em automation_logs
"""

import json
import os
import glob
import sys
from datetime import datetime
from pathlib import Path
import subprocess
import psycopg2
from dotenv import load_dotenv

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))
load_dotenv(SCRIPT_DIR / ".env")

from src.services.tender_notification_service import notify_users_of_new_tenders_batch
from src.utils.tender_enrichment import enrich_edital_scrape_data
from src.utils.tender_dates import coerce_date, parse_proposal_dates_from_text
from src.services.tender_expiration_service import expirar_licitacoes_encerradas


def _proposal_dates_from_edital(edital):
    start = coerce_date(edital.get('proposal_start_date'))
    end = coerce_date(edital.get('proposal_end_date'))
    if not start or not end:
        blob = ' '.join(
            filter(None, [edital.get('detailed_description'), edital.get('description'), edital.get('prazo')])
        )
        parsed_start, parsed_end = parse_proposal_dates_from_text(blob)
        start = start or parsed_start
        end = end or parsed_end
    return start, end


class AutomacaoLicitacoes:
    def __init__(self):
        os.chdir(SCRIPT_DIR)
        self.log_file = f"automacao_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        self.start_time = datetime.now()
        self.inseridos = 0
        self.scraper_states = os.getenv("SCRAPER_STATES", "SP,RJ,MG,RS,PR,SC,BA,GO,DF")
        self.scraper_limit = os.getenv("SCRAPER_LIMIT_PER_STATE", "10")
        self.scraper_timeout = int(os.getenv("SCRAPER_TIMEOUT_SECONDS", "7200"))

    def log(self, message, level="INFO"):
        """Adiciona mensagem ao log"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        log_message = f"[{timestamp}] [{level}] {message}"
        print(log_message)

        with open(self.log_file, 'a', encoding='utf-8') as f:
            f.write(log_message + '\n')

    def verificar_playwright(self):
        """Valida se Playwright e Chromium estão disponíveis antes do scrape."""
        self.log("🔍 Verificando Playwright...")
        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            self.log(
                "❌ Playwright não instalado. Execute: pip install playwright && "
                "python -m playwright install --with-deps chromium",
                "ERROR",
            )
            return False

        try:
            with sync_playwright() as playwright:
                browser = playwright.chromium.launch(
                    headless=True,
                    args=['--no-sandbox', '--disable-dev-shm-usage', '--disable-gpu'],
                )
                browser.close()
            self.log("✅ Playwright + Chromium OK")
            return True
        except Exception as exc:
            self.log(f"❌ Playwright/Chromium indisponível: {exc}", "ERROR")
            return False

    def executar_scraper(self):
        """Executa o scraper do PNCP"""
        self.log("=" * 60)
        self.log("FASE 1: Executando Scraper do PNCP")
        self.log("=" * 60)

        try:
            scraper_file = SCRIPT_DIR / 'pncp_scraper_items_only.py'
            if not scraper_file.exists():
                self.log(f"❌ Arquivo não encontrado: {scraper_file}", "ERROR")
                return None

            if not self.verificar_playwright():
                return None

            python_cmd = sys.executable
            cmd = [
                python_cmd,
                str(scraper_file),
                '--states', self.scraper_states,
                '--limit', self.scraper_limit,
                '--headless',
            ]

            self.log(f"✅ Scraper: {scraper_file.name}")
            self.log(f"📂 Diretório: {os.getcwd()}")
            self.log(f"🐍 Python: {python_cmd}")
            self.log(f"🌎 Estados: {self.scraper_states}")
            self.log(f"🔢 Limite/estado: {self.scraper_limit}")
            self.log(f"⏳ Executando scraper (timeout {self.scraper_timeout}s)...")

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                encoding='utf-8',
                errors='replace',
                timeout=self.scraper_timeout,
                cwd=str(SCRIPT_DIR),
            )

            self.log(f"✅ Scraper finalizou! Return code: {result.returncode}")

            if result.stdout:
                stdout_lines = result.stdout.strip().split('\n')
                self.log("📝 Output do scraper (últimas 10 linhas):")
                for line in stdout_lines[-10:]:
                    if line.strip():
                        self.log(f"  {line}")

            if result.stderr:
                stderr_lines = result.stderr.strip().split('\n')
                if any(line.strip() for line in stderr_lines):
                    self.log("⚠️  Stderr do scraper:")
                    for line in stderr_lines[-10:]:
                        if line.strip():
                            self.log(f"  {line}", "WARNING")

            if result.returncode != 0:
                self.log(f"❌ Scraper falhou com código: {result.returncode}", "ERROR")
                return None

            self.log("🔍 Procurando arquivo JSON gerado...")
            json_files = glob.glob(str(SCRIPT_DIR / 'editais_items_only_*.json'))

            if not json_files:
                self.log("❌ Nenhum arquivo JSON encontrado!", "ERROR")
                return None

            json_file = max(json_files, key=os.path.getctime)
            self.log(f"✅ JSON encontrado: {json_file}")

            file_size = os.path.getsize(json_file)
            self.log(f"📊 Tamanho do arquivo: {file_size:,} bytes")

            if file_size < 100:
                self.log("⚠️  Arquivo muito pequeno! Pode estar vazio.", "WARNING")

            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                self.log(f"✅ JSON válido com {len(data)} licitações")

                if len(data) == 0:
                    self.log("⚠️  JSON vazio! Nenhuma licitação encontrada.", "WARNING")

                return json_file

        except subprocess.TimeoutExpired:
            self.log(f"❌ Timeout ao executar scraper (>{self.scraper_timeout}s)", "ERROR")
            return None
        except Exception as e:
            self.log(f"❌ Erro inesperado no scraper: {e}", "ERROR")
            import traceback
            self.log(traceback.format_exc(), "ERROR")
            return None

    def inserir_no_banco(self, json_file):
        """Insere dados no banco de dados PostgreSQL"""
        self.log("=" * 60)
        self.log("FASE 2: Inserindo Dados no Banco")
        self.log("=" * 60)

        self.inseridos = 0

        try:
            if not json_file or not os.path.exists(json_file):
                self.log(f"❌ Arquivo JSON não encontrado: {json_file}", "ERROR")
                return False

            with open(json_file, 'r', encoding='utf-8') as f:
                json_data = json.load(f)

            self.log(f"📋 Licitações no JSON: {len(json_data)}")

            if len(json_data) == 0:
                self.log("⚠️  JSON vazio! Nada para inserir.", "WARNING")
                return True

            self.log("🔌 Conectando ao banco de dados...")
            conn = psycopg2.connect(
                host=os.getenv('DB_HOST'),
                port=os.getenv('DB_PORT', 5432),
                database=os.getenv('DB_NAME'),
                user=os.getenv('DB_USER'),
                password=os.getenv('DB_PASSWORD'),
                client_encoding='utf8',
            )
            cursor = conn.cursor()
            self.log("✅ Conectado ao banco!")

            cursor.execute("SELECT pncp_id FROM tenders WHERE pncp_id IS NOT NULL")
            existing_ids = set(row[0] for row in cursor.fetchall())
            self.log(f"📊 Licitações já no banco: {len(existing_ids)}")

            editais_validos = [e for e in json_data if e.get('pncp_id')]
            self.log(f"📋 Licitações válidas no JSON: {len(editais_validos)}")

            if len(editais_validos) == 0:
                self.log("✅ Nenhuma licitação válida para processar!")
                conn.close()
                return True

            inseridos = 0
            atualizados = 0
            erros = 0
            licitacoes_inseridas = []

            for i, edital in enumerate(editais_validos, 1):
                edital = enrich_edital_scrape_data(edital)
                title = edital.get('title', '')
                try:
                    pncp_id = edital.get('pncp_id', '')
                    description = edital.get('description', '')
                    organization_name = edital.get('organization_name', '')
                    organization_cnpj = edital.get('organization_cnpj', '')
                    municipality_name = edital.get('municipality_name', '')
                    municipality_ibge = edital.get('municipality_ibge', '')
                    state_code = edital.get('state_code', '')
                    publication_date = edital.get('publication_date')
                    status = edital.get('status', '')
                    modality = edital.get('modality', '')
                    estimated_value = edital.get('estimated_value')
                    source_url = edital.get('source_url', '')
                    detail_url = edital.get('detail_url', '')
                    data_source = f'PNCP_SCRAPING_{datetime.now().strftime("%Y%m%d")}'

                    objeto = edital.get('objeto', '')
                    detailed_description = edital.get('detailed_description', '')
                    valor_total_estimado = edital.get('valor_total_estimado')
                    prazo = edital.get('prazo', '')

                    items = edital.get('items', [])
                    items_json = json.dumps(items, ensure_ascii=False) if items else None
                    items_count = len(items) if items else 0

                    downloaded_files = edital.get('downloaded_files', [])
                    downloaded_files_json = json.dumps(downloaded_files, ensure_ascii=False) if downloaded_files else None
                    downloads_count = len(downloaded_files) if downloaded_files else 0

                    created_at = datetime.now()

                    if publication_date and isinstance(publication_date, str):
                        try:
                            publication_date = datetime.strptime(publication_date, '%Y-%m-%d').date()
                        except Exception:
                            publication_date = None

                    if estimated_value is None and valor_total_estimado is not None:
                        estimated_value = valor_total_estimado

                    proposal_start_date, proposal_end_date = _proposal_dates_from_edital(edital)

                    if pncp_id in existing_ids:
                        cursor.execute("""
                            UPDATE tenders SET
                                title = COALESCE(NULLIF(%s, ''), title),
                                description = COALESCE(NULLIF(%s, ''), description),
                                organization_name = COALESCE(NULLIF(%s, ''), organization_name),
                                organization_cnpj = COALESCE(NULLIF(%s, ''), organization_cnpj),
                                municipality_name = COALESCE(NULLIF(%s, ''), municipality_name),
                                municipality_ibge = COALESCE(NULLIF(%s, ''), municipality_ibge),
                                state_code = COALESCE(NULLIF(%s, ''), state_code),
                                publication_date = COALESCE(%s, publication_date),
                                status = COALESCE(NULLIF(%s, ''), status),
                                modality = COALESCE(NULLIF(%s, ''), modality),
                                estimated_value = COALESCE(%s, estimated_value),
                                source_url = COALESCE(NULLIF(%s, ''), source_url),
                                detail_url = COALESCE(NULLIF(%s, ''), detail_url),
                                data_source = %s,
                                objeto = COALESCE(NULLIF(%s, ''), objeto),
                                detailed_description = COALESCE(NULLIF(%s, ''), detailed_description),
                                valor_total_estimado = COALESCE(%s, valor_total_estimado),
                                prazo = COALESCE(NULLIF(%s, ''), prazo),
                                proposal_start_date = COALESCE(%s, proposal_start_date),
                                proposal_end_date = COALESCE(%s, proposal_end_date),
                                items_json = CASE WHEN %s > 0 THEN %s ELSE items_json END,
                                items_count = GREATEST(COALESCE(items_count, 0), %s),
                                downloaded_files_json = CASE WHEN %s > 0 THEN %s ELSE downloaded_files_json END,
                                downloads_count = GREATEST(COALESCE(downloads_count, 0), %s)
                            WHERE pncp_id = %s
                        """, (
                            title, description, organization_name, organization_cnpj,
                            municipality_name, municipality_ibge, state_code, publication_date,
                            status, modality, estimated_value, source_url, detail_url,
                            data_source, objeto, detailed_description, valor_total_estimado, prazo,
                            proposal_start_date, proposal_end_date,
                            items_count, items_json, items_count,
                            downloads_count, downloaded_files_json, downloads_count,
                            pncp_id,
                        ))
                        conn.commit()
                        atualizados += 1
                    else:
                        cursor.execute("""
                            INSERT INTO tenders (
                                pncp_id, title, description, organization_name, organization_cnpj,
                                municipality_name, municipality_ibge, state_code, publication_date,
                                status, modality, estimated_value, source_url, detail_url,
                                data_source, created_at,
                                objeto, detailed_description, valor_total_estimado, prazo,
                                proposal_start_date, proposal_end_date,
                                items_json, items_count, downloaded_files_json, downloads_count
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            RETURNING id
                        """, (
                            pncp_id, title, description, organization_name, organization_cnpj,
                            municipality_name, municipality_ibge, state_code, publication_date,
                            status, modality, estimated_value, source_url, detail_url,
                            data_source, created_at,
                            objeto, detailed_description, valor_total_estimado, prazo,
                            proposal_start_date, proposal_end_date,
                            items_json, items_count, downloaded_files_json, downloads_count,
                        ))
                        tender_id = cursor.fetchone()[0]
                        conn.commit()
                        inseridos += 1
                        existing_ids.add(pncp_id)
                        licitacoes_inseridas.append({
                            'id': tender_id,
                            'title': title,
                            'description': description,
                            'objeto': objeto,
                            'organization_name': organization_name,
                            'municipality_name': municipality_name,
                            'state_code': state_code,
                            'modality': modality,
                            'estimated_value': estimated_value or valor_total_estimado,
                            'publication_date': publication_date,
                            'detail_url': detail_url,
                        })

                    if i % 5 == 0 or i == len(editais_validos):
                        self.log(f"  ✅ Progresso: {i}/{len(editais_validos)} (inseridos: {inseridos}, atualizados: {atualizados})")

                except Exception as e:
                    erros += 1
                    conn.rollback()
                    self.log(f"  ❌ Erro ao processar '{title[:30]}...': {e}", "ERROR")

            self.inseridos = inseridos

            if licitacoes_inseridas:
                self.log(f"📧 Enviando notificações por email ({len(licitacoes_inseridas)} licitações)...")
                resultado = notify_users_of_new_tenders_batch(licitacoes_inseridas, wait=True)
                if resultado:
                    self.log(
                        f"📧 Resumos: {resultado.get('emails_sent', 0)} email(s) enviado(s) "
                        f"({resultado.get('tenders_notified', 0)} licitações), "
                        f"{resultado.get('emails_failed', 0)} falha(s), "
                        f"{resultado.get('skipped', 0)} já notificadas"
                    )

            cursor.execute("SELECT COUNT(*) FROM tenders")
            total_final = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM tenders WHERE objeto IS NOT NULL AND objeto != ''")
            com_objeto = cursor.fetchone()[0]

            conn.close()

            self.log("")
            self.log("📊 Inserção concluída!")
            self.log(f"  ✅ Inseridos: {inseridos}")
            self.log(f"  🔄 Atualizados: {atualizados}")
            self.log(f"  ❌ Erros: {erros}")
            self.log(f"  📈 Total no banco: {total_final}")
            self.log(f"  📝 Com objeto: {com_objeto}")

            return True

        except Exception as e:
            self.log(f"❌ Erro ao inserir no banco: {e}", "ERROR")
            import traceback
            self.log(traceback.format_exc(), "ERROR")
            return False

    def limpar_arquivos_antigos(self, dias=7):
        """Remove JSONs e logs antigos"""
        self.log("=" * 60)
        self.log("FASE 3: Limpando Arquivos Antigos")
        self.log("=" * 60)

        try:
            import time

            now = time.time()
            cutoff = now - (dias * 86400)

            json_files = glob.glob(str(SCRIPT_DIR / 'editais_items_only_*.json'))
            json_removidos = 0

            for f in json_files:
                if os.path.getctime(f) < cutoff:
                    os.remove(f)
                    json_removidos += 1

            log_files = glob.glob(str(SCRIPT_DIR / 'automacao_log_*.txt'))
            log_removidos = 0

            for f in log_files:
                if f != self.log_file and os.path.getctime(f) < cutoff:
                    os.remove(f)
                    log_removidos += 1

            self.log("✅ Limpeza concluída:")
            self.log(f"  📄 JSONs removidos: {json_removidos}")
            self.log(f"  📋 Logs removidos: {log_removidos}")

        except Exception as e:
            self.log(f"❌ Erro ao limpar arquivos: {e}", "ERROR")

    def salvar_estatisticas(self, sucesso, json_file=None, novos=0):
        """Salva estatísticas da execução no banco"""
        try:
            conn = psycopg2.connect(
                host=os.getenv('DB_HOST'),
                port=os.getenv('DB_PORT', 5432),
                database=os.getenv('DB_NAME'),
                user=os.getenv('DB_USER'),
                password=os.getenv('DB_PASSWORD'),
            )
            cursor = conn.cursor()

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS automation_logs (
                    id SERIAL PRIMARY KEY,
                    execution_date TIMESTAMP DEFAULT NOW(),
                    status VARCHAR(20),
                    new_tenders INT,
                    execution_time_seconds INT,
                    json_file VARCHAR(255),
                    log_file VARCHAR(255)
                )
            """)

            execution_time = int((datetime.now() - self.start_time).total_seconds())

            cursor.execute("""
                INSERT INTO automation_logs (
                    status, new_tenders, execution_time_seconds, json_file, log_file
                ) VALUES (%s, %s, %s, %s, %s)
            """, (
                'success' if sucesso else 'error',
                novos,
                execution_time,
                json_file,
                self.log_file,
            ))

            conn.commit()
            conn.close()

            self.log("✅ Estatísticas salvas no banco!")

        except Exception as e:
            self.log(f"⚠️  Erro ao salvar estatísticas: {e}", "WARNING")

    def executar(self):
        """Executa todo o processo de automação"""
        self.log("🤖 AUTOMAÇÃO DE LICITAÇÕES - INÍCIO")
        self.log(f"📅 Data/Hora: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        self.log(f"📂 Diretório: {os.getcwd()}")
        self.log(f"🐍 Python: {sys.executable}")

        try:
            json_file = self.executar_scraper()
            if not json_file:
                self.log("❌ Falha no scraper. Abortando.", "ERROR")
                self.salvar_estatisticas(False)
                return False

            sucesso = self.inserir_no_banco(json_file)
            if not sucesso:
                self.log("❌ Falha na inserção. Abortando.", "ERROR")
                self.salvar_estatisticas(False, json_file)
                return False

            self.log("=" * 60)
            self.log("FASE 2.5: Expirando licitações encerradas")
            self.log("=" * 60)
            try:
                expiradas = expirar_licitacoes_encerradas()
                self.log(f"⏳ Licitações marcadas como Expirada: {expiradas}")
            except Exception as e:
                self.log(f"⚠️  Erro ao expirar licitações: {e}", "WARNING")

            self.limpar_arquivos_antigos(dias=7)
            self.salvar_estatisticas(True, json_file, self.inseridos)

            tempo_total = (datetime.now() - self.start_time).total_seconds()
            self.log("=" * 60)
            self.log("✅ AUTOMAÇÃO CONCLUÍDA COM SUCESSO!")
            self.log(f"⏱️  Tempo total: {int(tempo_total)} segundos ({int(tempo_total/60)} minutos)")
            self.log(f"🆕 Novas licitações inseridas: {self.inseridos}")
            self.log(f"📋 Log salvo em: {self.log_file}")
            self.log("=" * 60)

            return True

        except Exception as e:
            self.log(f"❌ Erro inesperado: {e}", "ERROR")
            import traceback
            self.log(traceback.format_exc(), "ERROR")
            self.salvar_estatisticas(False)
            return False


if __name__ == "__main__":
    automacao = AutomacaoLicitacoes()
    sucesso = automacao.executar()
    sys.exit(0 if sucesso else 1)
