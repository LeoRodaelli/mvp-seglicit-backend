#!/usr/bin/env python3
"""
Script de Automação Completa - Licitações em Tempo Real (VERSÃO FINAL)
"""

import subprocess
import json
import os
import glob
import sys
from datetime import datetime
from pathlib import Path
import psycopg2
from dotenv import load_dotenv

load_dotenv()


class AutomacaoLicitacoes:
    def __init__(self):
        self.log_file = f"automacao_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        self.start_time = datetime.now()
        
    def log(self, message, level="INFO"):
        """Adiciona mensagem ao log"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        log_message = f"[{timestamp}] [{level}] {message}"
        print(log_message)
        
        with open(self.log_file, 'a', encoding='utf-8') as f:
            f.write(log_message + '\n')
    
    def executar_scraper(self):
        """Executa o scraper do PNCP"""
        self.log("=" * 60)
        self.log("FASE 1: Executando Scraper do PNCP")
        self.log("=" * 60)
        
        try:
            # Verificar se arquivo existe
            scraper_file = 'pncp_scraper_items_only.py'
            if not os.path.exists(scraper_file):
                self.log(f"❌ Arquivo não encontrado: {scraper_file}", "ERROR")
                self.log(f"Diretório atual: {os.getcwd()}", "ERROR")
                return None
            
            self.log(f"✅ Arquivo encontrado: {scraper_file}")
            self.log(f"📂 Diretório: {os.getcwd()}")
            
            # Verificar Python
            python_cmd = sys.executable
            self.log(f"🐍 Python: {python_cmd}")
            
            # Executar scraper (VERSÃO CORRIGIDA - não trava!)
            self.log(f"⏳ Executando: {python_cmd} {scraper_file}")
            self.log("(Isso pode levar vários minutos... aguarde!)")
            
            # Usar subprocess.run ao invés de Popen
            result = subprocess.run(
                [python_cmd, scraper_file],
                capture_output=True,
                text=True,
                encoding='utf-8',
                errors='replace',
                timeout=3600  # 1 hora de timeout
            )
            
            self.log(f"✅ Scraper finalizou! Return code: {result.returncode}")
            
            # Mostrar últimas linhas do output
            if result.stdout:
                stdout_lines = result.stdout.strip().split('\n')
                self.log(f"📝 Output do scraper (últimas 10 linhas):")
                for line in stdout_lines[-10:]:
                    if line.strip():
                        self.log(f"  {line}")
            
            # Mostrar erros se houver
            if result.stderr:
                stderr_lines = result.stderr.strip().split('\n')
                if any(line.strip() for line in stderr_lines):
                    self.log(f"⚠️  Stderr do scraper:")
                    for line in stderr_lines[-10:]:
                        if line.strip():
                            self.log(f"  {line}", "WARNING")
            
            if result.returncode != 0:
                self.log(f"❌ Scraper falhou com código: {result.returncode}", "ERROR")
                return None
            
            # Encontrar JSON mais recente
            self.log("🔍 Procurando arquivo JSON gerado...")
            json_files = glob.glob('editais_items_only_*.json')
            
            if not json_files:
                self.log("❌ Nenhum arquivo JSON encontrado!", "ERROR")
                return None
            
            # Pegar o mais recente
            json_file = max(json_files, key=os.path.getctime)
            self.log(f"✅ JSON encontrado: {json_file}")
            
            # Verificar tamanho
            file_size = os.path.getsize(json_file)
            self.log(f"📊 Tamanho do arquivo: {file_size:,} bytes")
            
            if file_size < 100:
                self.log(f"⚠️  Arquivo muito pequeno! Pode estar vazio.", "WARNING")
            
            # Verificar conteúdo
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.log(f"✅ JSON válido com {len(data)} licitações")
                    
                    if len(data) == 0:
                        self.log("⚠️  JSON vazio! Nenhuma licitação encontrada.", "WARNING")
                    
                    return json_file
            except json.JSONDecodeError as e:
                self.log(f"❌ JSON inválido: {e}", "ERROR")
                return None
            
        except subprocess.TimeoutExpired:
            self.log("❌ Timeout ao executar scraper (>1 hora)", "ERROR")
            return None
        except FileNotFoundError as e:
            self.log(f"❌ Arquivo não encontrado: {e}", "ERROR")
            return None
        except Exception as e:
            self.log(f"❌ Erro inesperado no scraper: {e}", "ERROR")
            import traceback
            self.log(traceback.format_exc(), "ERROR")
            return None
    
    def inserir_no_banco(self, json_file):
        """Insere dados no banco de dados"""
        self.log("=" * 60)
        self.log("FASE 2: Inserindo Dados no Banco")
        self.log("=" * 60)
        
        try:
            if not json_file or not os.path.exists(json_file):
                self.log(f"❌ Arquivo JSON não encontrado: {json_file}", "ERROR")
                return False
            
            # Carregar JSON
            with open(json_file, 'r', encoding='utf-8') as f:
                json_data = json.load(f)
            
            self.log(f"📋 Licitações no JSON: {len(json_data)}")
            
            if len(json_data) == 0:
                self.log("⚠️  JSON vazio! Nada para inserir.", "WARNING")
                return True
            
            # Conectar ao banco
            self.log("🔌 Conectando ao banco de dados...")
            conn = psycopg2.connect(
                host=os.getenv('DB_HOST'),
                port=os.getenv('DB_PORT', 5432),
                database=os.getenv('DB_NAME'),
                user=os.getenv('DB_USER'),
                password=os.getenv('DB_PASSWORD'),
                client_encoding='utf8'
            )
            cursor = conn.cursor()
            self.log("✅ Conectado ao banco!")
            
            # Obter IDs existentes
            cursor.execute("SELECT pncp_id FROM tenders WHERE pncp_id IS NOT NULL")
            existing_ids = set(row[0] for row in cursor.fetchall())
            self.log(f"📊 Licitações já no banco: {len(existing_ids)}")
            
            # Filtrar novas
            novos_editais = [e for e in json_data if e.get('pncp_id') and e.get('pncp_id') not in existing_ids]
            self.log(f"🆕 Licitações novas para adicionar: {len(novos_editais)}")
            
            if len(novos_editais) == 0:
                self.log("✅ Nenhuma licitação nova encontrada!")
                conn.close()
                return True
            
            # Inserir
            inseridos = 0
            erros = 0
            
            for i, edital in enumerate(novos_editais, 1):
                try:
                    # Preparar dados
                    pncp_id = edital.get('pncp_id', '')
                    title = edital.get('title', '')
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
                    items_json = json.dumps(items) if items else None
                    items_count = len(items) if items else 0
                    
                    downloaded_files = edital.get('downloaded_files', [])
                    downloaded_files_json = json.dumps(downloaded_files) if downloaded_files else None
                    downloads_count = len(downloaded_files) if downloaded_files else 0
                    
                    created_at = datetime.now()
                    
                    # Tratar data
                    if publication_date and isinstance(publication_date, str):
                        try:
                            publication_date = datetime.strptime(publication_date, '%Y-%m-%d').date()
                        except:
                            publication_date = None
                    
                    if estimated_value is None and valor_total_estimado is not None:
                        estimated_value = valor_total_estimado
                    
                    # Inserir
                    cursor.execute("""
                        INSERT INTO tenders (
                            pncp_id, title, description, organization_name, organization_cnpj,
                            municipality_name, municipality_ibge, state_code, publication_date,
                            status, modality, estimated_value, source_url, detail_url,
                            data_source, created_at,
                            objeto, detailed_description, valor_total_estimado, prazo,
                            items_json, items_count, downloaded_files_json, downloads_count
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        pncp_id, title, description, organization_name, organization_cnpj,
                        municipality_name, municipality_ibge, state_code, publication_date,
                        status, modality, estimated_value, source_url, detail_url,
                        data_source, created_at,
                        objeto, detailed_description, valor_total_estimado, prazo,
                        items_json, items_count, downloaded_files_json, downloads_count
                    ))
                    
                    inseridos += 1
                    if i % 5 == 0 or i == len(novos_editais):
                        self.log(f"  ✅ Progresso: {i}/{len(novos_editais)} licitações inseridas")
                    
                except Exception as e:
                    erros += 1
                    self.log(f"  ❌ Erro ao inserir '{title[:30]}...': {e}", "ERROR")
                    conn.rollback()
                    continue
            
            conn.commit()
            
            # Estatísticas finais
            cursor.execute("SELECT COUNT(*) FROM tenders")
            total_final = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM tenders WHERE objeto IS NOT NULL AND objeto != ''")
            com_objeto = cursor.fetchone()[0]
            
            conn.close()
            
            self.log(f"")
            self.log(f"📊 Inserção concluída!")
            self.log(f"  ✅ Inseridos: {inseridos}")
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
            
            # Limpar JSONs
            json_files = glob.glob('editais_items_only_*.json')
            json_removidos = 0
            
            for f in json_files:
                if os.path.getctime(f) < cutoff:
                    os.remove(f)
                    json_removidos += 1
            
            # Limpar logs
            log_files = glob.glob('automacao_log_*.txt')
            log_removidos = 0
            
            for f in log_files:
                if f != self.log_file and os.path.getctime(f) < cutoff:
                    os.remove(f)
                    log_removidos += 1
            
            self.log(f"✅ Limpeza concluída:")
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
                password=os.getenv('DB_PASSWORD')
            )
            cursor = conn.cursor()
            
            # Criar tabela se não existir
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
            
            # Calcular tempo
            execution_time = int((datetime.now() - self.start_time).total_seconds())
            
            # Inserir log
            cursor.execute("""
                INSERT INTO automation_logs (
                    status, new_tenders, execution_time_seconds, json_file, log_file
                ) VALUES (%s, %s, %s, %s, %s)
            """, (
                'success' if sucesso else 'error',
                novos,
                execution_time,
                json_file,
                self.log_file
            ))
            
            conn.commit()
            conn.close()
            
            self.log(f"✅ Estatísticas salvas no banco!")
            
        except Exception as e:
            self.log(f"⚠️  Erro ao salvar estatísticas: {e}", "WARNING")
    
    def executar(self):
        """Executa todo o processo de automação"""
        self.log("🤖 AUTOMAÇÃO DE LICITAÇÕES - INÍCIO")
        self.log(f"📅 Data/Hora: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        self.log(f"📂 Diretório: {os.getcwd()}")
        self.log(f"🐍 Python: {sys.executable}")
        
        try:
            # Fase 1: Scraper
            json_file = self.executar_scraper()
            if not json_file:
                self.log("❌ Falha no scraper. Abortando.", "ERROR")
                self.salvar_estatisticas(False)
                return False
            
            # Fase 2: Inserção
            sucesso = self.inserir_no_banco(json_file)
            if not sucesso:
                self.log("❌ Falha na inserção. Abortando.", "ERROR")
                self.salvar_estatisticas(False, json_file)
                return False
            
            # Contar novos
            try:
                with open(json_file, 'r') as f:
                    data = json.load(f)
                    novos = len(data)
            except:
                novos = 0
            
            # Fase 3: Limpeza
            self.limpar_arquivos_antigos(dias=7)
            
            # Salvar estatísticas
            self.salvar_estatisticas(True, json_file, novos)
            
            # Tempo total
            tempo_total = (datetime.now() - self.start_time).total_seconds()
            self.log("=" * 60)
            self.log(f"✅ AUTOMAÇÃO CONCLUÍDA COM SUCESSO!")
            self.log(f"⏱️  Tempo total: {int(tempo_total)} segundos ({int(tempo_total/60)} minutos)")
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
