#!/usr/bin/env python3
"""
PNCP Scraper - Extrai itens APENAS da aba "Itens" ativa
"""

import asyncio
import os
import json
import sqlite3
from datetime import datetime, date
from typing import List, Dict, Optional
import re
import logging
from pathlib import Path

try:
    from playwright.async_api import async_playwright, Page, Browser
except ImportError:
    print("ERRO: Playwright nao instalado. Execute: pip install playwright")
    print("   Depois execute: playwright install")
    exit(1)

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pncp_scraper.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class PNCPScraperItemsOnly:
    """Scraper que extrai itens APENAS da aba 'Itens' ativa"""
    
    def __init__(self, headless: bool = False):
        self.headless = headless
        self.base_url = "https://pncp.gov.br"
        self.editais_url = f"{self.base_url}/app/editais?pagina=1"
        self.browser: Optional[Browser] = None
        self.page: Optional[Page] = None
        self.download_dir = "downloads"
        
        # Criar diretório de downloads
        os.makedirs(self.download_dir, exist_ok=True)
        
    async def __aenter__(self):
        await self.start()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
    
    async def start(self):
        """Inicializa o navegador"""
        logger.info("🧭 Iniciando browser...")
        
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(
            headless=self.headless,
            args=[
                '--no-sandbox', 
                '--disable-dev-shm-usage'
            ]
        )
        
        # Configurar contexto com downloads
        context = await self.browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={'width': 1920, 'height': 1080},
            accept_downloads=True
        )
        
        self.page = await context.new_page()
        self.page.set_default_timeout(60000)
        
        logger.info("✅ Navegador iniciado com sucesso")
    
    async def close(self):
        """Fecha o navegador"""
        if self.browser:
            await self.browser.close()
        if hasattr(self, 'playwright'):
            await self.playwright.stop()
        logger.info("🔒 Navegador fechado")
    
    async def navigate_and_filter(self, uf: str) -> bool:
        """Navega para página e aplica filtro UF"""
        try:
            logger.info("🌐 Acessando a página de editais...")
            await self.page.goto(self.editais_url, timeout=60000)
            await self.page.wait_for_timeout(5000)
            
            # Screenshot inicial
            await self.page.screenshot(path="debug_01_inicial.png")
            logger.info("📸 Screenshot inicial salvo")
            
            logger.info(f"📍 Selecionando UF: {uf}...")
            
            # Clicar no dropdown UF
            await self.page.click('pncp-select#ufs .ng-select-container')
            await self.page.wait_for_timeout(1000)
            
            # Preencher campo de busca
            await self.page.locator("pncp-select#ufs input[type='text']").fill(uf)
            await self.page.wait_for_timeout(1000)
            
            # Pressionar Enter
            await self.page.keyboard.press("Enter")
            await self.page.wait_for_timeout(2000)
            
            # Screenshot após selecionar UF
            await self.page.screenshot(path="debug_02_uf_selecionado.png")
            logger.info("📸 Screenshot UF selecionado salvo")
            
            logger.info("🔎 Clicando no botão Pesquisar...")
            await self.page.click("button.br-button.primary:has-text('Pesquisar')")
            await self.page.wait_for_timeout(5000)
            
            # Screenshot após pesquisar
            await self.page.screenshot(path="debug_03_resultados.png")
            logger.info("📸 Screenshot resultados salvo")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Erro ao navegar e filtrar: {e}")
            return False
    
    async def get_editais_count(self) -> int:
        """Conta quantos editais estão disponíveis"""
        try:
            await self.page.wait_for_timeout(3000)
            
            cards = await self.page.locator("a.br-item").all()
            count = len(cards)
            
            logger.info(f"📊 Encontrados {count} editais na página (seletor: a.br-item)")
            
            if count == 0:
                alternative_selectors = [
                    'a[class="br-item"]',
                    'a[title="Acessar item."]',
                    'a[href*="/editais/"]',
                    '.br-item'
                ]
                
                for selector in alternative_selectors:
                    try:
                        cards = await self.page.locator(selector).all()
                        if len(cards) > 0:
                            logger.info(f"📊 Encontrados {len(cards)} editais com seletor alternativo: {selector}")
                            return len(cards)
                    except:
                        continue
            
            return count
            
        except Exception as e:
            logger.error(f"❌ Erro ao contar editais: {e}")
            return 0
    
    async def process_edital(self, index: int) -> Optional[Dict]:
        """Processa um edital específico pelo índice"""
        try:
            logger.info(f"\n{'='*50}")
            logger.info(f"📄 PROCESSANDO EDITAL {index + 1}")
            logger.info(f"{'='*50}")
            
            await self.page.wait_for_timeout(2000)
            
            # Obter todos os cards
            cards = await self.page.locator("a.br-item").all()
            
            if len(cards) == 0:
                alternative_selectors = [
                    'a[class="br-item"]',
                    'a[title="Acessar item."]',
                    'a[href*="/editais/"]'
                ]
                
                for selector in alternative_selectors:
                    try:
                        cards = await self.page.locator(selector).all()
                        if len(cards) > 0:
                            logger.info(f"✅ Usando seletor alternativo: {selector}")
                            break
                    except:
                        continue
            
            if index >= len(cards):
                logger.warning(f"⚠️ Índice {index} fora do range. Total de cards: {len(cards)}")
                return None
            
            # Extrair informações básicas do card
            card = cards[index]
            card_text = await card.inner_text()
            logger.info(f"📋 Texto do card: {card_text[:100]}...")
            
            href = await card.get_attribute('href')
            logger.info(f"🔗 Link do edital: {href}")
            
            # Extrair informações básicas
            edital_info = self.extract_basic_info(card_text, index)
            edital_info['edital_href'] = href
            
            # Screenshot antes de clicar
            await self.page.screenshot(path=f"debug_04_antes_click_{index}.png")
            
            # Clicar no card para acessar detalhes
            logger.info(f"🖱️ Clicando no edital {index + 1}...")
            await card.click()
            await self.page.wait_for_timeout(4000)
            
            # Screenshot da página de detalhes
            await self.page.screenshot(path=f"debug_05_detalhes_{index}.png")
            
            current_url = self.page.url
            logger.info(f"📍 URL atual: {current_url}")
            
            # Extrair informações detalhadas da página
            detailed_info = await self.extract_detailed_info()
            
            # CORRIGIDO: Processar aba "Itens" APENAS para dados da tabela
            items_info = await self.process_items_tab_only(index)
            
            # Depois processar aba "Arquivos" para downloads
            files_info = await self.process_files_tab(index)
            
            # Tentar acessar contratação
            access_info = await self.try_access_contratacao(index)
            
            # Combinar todas as informações
            edital_info.update(detailed_info)
            edital_info.update(items_info)
            edital_info.update(files_info)
            edital_info.update(access_info)
            edital_info['detail_url'] = current_url
            
            logger.info(f"✅ Edital {index + 1} processado com sucesso!")
            logger.info(f"   Título: {edital_info.get('title', 'N/A')}")
            logger.info(f"   Organização: {edital_info.get('organization_name', 'N/A')}")
            logger.info(f"   Itens na aba Itens: {len(edital_info.get('items', []))}")
            logger.info(f"   Arquivos baixados: {len(edital_info.get('downloaded_files', []))}")
            
            # Voltar para página de resultados
            logger.info(f"🔙 Voltando para lista de editais...")
            await self.page.go_back()
            await self.page.wait_for_timeout(3000)
            
            # Screenshot após voltar
            await self.page.screenshot(path=f"debug_06_voltou_{index}.png")
            
            return edital_info
            
        except Exception as e:
            logger.error(f"❌ Erro ao processar edital {index + 1}: {e}")
            
            try:
                await self.page.go_back()
                await self.page.wait_for_timeout(2000)
            except:
                pass
                
            return None
    
    async def process_items_tab_only(self, index: int) -> Dict:
        """CORRIGIDO: Processa APENAS a aba 'Itens' ativa para extrair dados da tabela"""
        try:
            logger.info("📊 Processando APENAS aba 'Itens' ativa para extrair dados...")
            
            # Screenshot antes de procurar aba Itens
            await self.page.screenshot(path=f"debug_07_antes_aba_itens_{index}.png")
            
            # Garantir que estamos na aba "Itens"
            await self.ensure_items_tab_active()
            
            # Aguardar carregamento da tabela da aba Itens
            await self.page.wait_for_timeout(3000)
            
            # Screenshot da aba Itens ativa
            await self.page.screenshot(path=f"debug_08_aba_itens_ativa_{index}.png")
            
            # CORRIGIDO: Extrair itens APENAS da tabela visível da aba Itens
            items = await self.extract_items_from_active_items_tab(index)
            
            return {
                'items_tab_found': True,
                'items': items,
                'items_count': len(items)
            }
            
        except Exception as e:
            logger.error(f"❌ Erro ao processar aba Itens: {e}")
            return {'items_tab_found': False, 'items': [], 'items_count': 0}
    
    async def ensure_items_tab_active(self):
        """Garante que a aba 'Itens' está ativa"""
        try:
            logger.info("🎯 Garantindo que aba 'Itens' está ativa...")
            
            # Procurar pela aba "Itens"
            items_tab_selectors = [
                'li.tab-item:has-text("Itens")',
                'button:has-text("Itens")',
                '[class*="tab"]:has-text("Itens")',
                'li:has-text("Itens") button',
                '.tab-item:has-text("Itens") button'
            ]
            
            for selector in items_tab_selectors:
                try:
                    tab_button = self.page.locator(selector)
                    
                    if await tab_button.count() > 0:
                        logger.info(f"✅ Aba 'Itens' encontrada! (Seletor: {selector})")
                        
                        # Verificar se já está ativa
                        tab_element = await tab_button.first.element_handle()
                        if tab_element:
                            class_attr = await tab_element.get_attribute('class')
                            if 'is-active' in (class_attr or ''):
                                logger.info("📊 Aba 'Itens' já está ativa")
                            else:
                                logger.info("🖱️ Clicando na aba 'Itens' para ativá-la...")
                                await tab_button.click()
                                await self.page.wait_for_timeout(3000)
                                logger.info("✅ Aba 'Itens' ativada")
                        
                        return True
                        
                except Exception as e:
                    logger.warning(f"⚠️ Erro com seletor de aba Itens {selector}: {e}")
                    continue
            
            logger.warning("⚠️ Aba 'Itens' não encontrada, assumindo que já está ativa")
            return False
            
        except Exception as e:
            logger.error(f"❌ Erro ao garantir aba Itens ativa: {e}")
            return False
    
    async def extract_items_from_active_items_tab(self, index: int) -> List[Dict]:
        """CORRIGIDO: Extrai itens APENAS da tabela visível da aba 'Itens' ativa"""
        try:
            logger.info("📊 Extraindo itens APENAS da tabela visível da aba 'Itens'...")
            
            # Aguardar carregamento da tabela
            await self.page.wait_for_timeout(3000)
            
            # Screenshot da tabela na aba Itens
            await self.page.screenshot(path=f"debug_09_tabela_itens_ativa_{index}.png")
            
            items = []
            
            # ESTRATÉGIA 1: Procurar por container específico da aba Itens ativa
            items_container_selectors = [
                # Container da aba Itens que está visível
                'div[role="tabpanel"]:not([hidden]) datatable-body-row',
                'div[aria-hidden="false"] datatable-body-row',
                'div.tab-content:not(.d-none) datatable-body-row',
                'div.active datatable-body-row',
                # Fallback para tabela visível
                'datatable-body-row:visible',
                'datatable-body-row'
            ]
            
            rows_found = False
            
            for selector in items_container_selectors:
                try:
                    logger.info(f"🔍 Testando seletor: {selector}")
                    
                    # Aguardar que a tabela carregue
                    await self.page.wait_for_timeout(2000)
                    
                    rows = await self.page.locator(selector).all()
                    
                    if len(rows) > 0:
                        logger.info(f"📊 Encontradas {len(rows)} linhas com seletor: {selector}")
                        
                        # VALIDAÇÃO: Verificar se as linhas são realmente da aba Itens
                        valid_rows = []
                        
                        for i, row in enumerate(rows):
                            try:
                                # Verificar se a linha está visível (não oculta)
                                is_visible = await row.is_visible()
                                
                                if not is_visible:
                                    logger.info(f"   ⚠️ Linha {i+1} não está visível (aba inativa), ignorando...")
                                    continue
                                
                                # Extrair texto da linha para validação
                                row_text = await row.inner_text()
                                
                                # VALIDAÇÃO: Verificar se é uma linha de item válida (não de histórico/arquivos)
                                if self.is_valid_items_row(row_text):
                                    valid_rows.append(row)
                                    logger.info(f"   ✅ Linha {i+1} válida: {row_text[:50]}...")
                                else:
                                    logger.info(f"   ❌ Linha {i+1} inválida (histórico/arquivo): {row_text[:50]}...")
                                
                            except Exception as e:
                                logger.warning(f"⚠️ Erro ao validar linha {i}: {e}")
                                continue
                        
                        # Processar apenas linhas válidas da aba Itens
                        if valid_rows:
                            logger.info(f"📊 Processando {len(valid_rows)} linhas válidas da aba 'Itens'")
                            
                            for i, row in enumerate(valid_rows[:10]):  # Processar até 10 itens
                                try:
                                    item_data = await self.extract_angular_row_data(row, i)
                                    
                                    if item_data:
                                        items.append(item_data)
                                        logger.info(f"   📋 Item {i+1}: {item_data.get('descricao', 'N/A')[:50]}...")
                                    
                                except Exception as e:
                                    logger.warning(f"⚠️ Erro ao processar linha válida {i}: {e}")
                                    continue
                            
                            rows_found = True
                            break
                        else:
                            logger.info(f"   ⚠️ Nenhuma linha válida encontrada com seletor {selector}")
                            
                except Exception as e:
                    logger.warning(f"⚠️ Erro com seletor {selector}: {e}")
                    continue
            
            # Se não encontrou com seletores específicos, tentar fallback mais restritivo
            if not rows_found:
                logger.info("📊 Tentando fallback: extrair itens do texto visível da aba Itens...")
                items = await self.extract_items_from_visible_text(index)
            
            logger.info(f"📊 Total de itens extraídos da aba 'Itens' ativa: {len(items)}")
            
            return items
            
        except Exception as e:
            logger.error(f"❌ Erro ao extrair tabela da aba Itens ativa: {e}")
            return []
    
    def is_valid_items_row(self, row_text: str) -> bool:
        """NOVO: Valida se uma linha é realmente um item da aba Itens (não histórico/arquivo)"""
        try:
            # Remover espaços e quebras de linha
            text = row_text.strip().lower()
            
            # REJEITAR linhas que são claramente de outras abas
            invalid_patterns = [
                # Padrões da aba Histórico
                r'\d{2}/\d{2}/\d{4}.*\d{2}:\d{2}:\d{2}',  # Data com hora (histórico)
                r'inclusão.*contratação',  # "Inclusão - Contratação"
                r'inclusão.*documento',  # "Inclusão - Documento"
                r'alteração.*',  # Alterações do histórico
                r'publicação.*',  # Publicações do histórico
                
                # Padrões da aba Arquivos
                r'\.pdf$',  # Nomes de arquivo PDF
                r'\.doc$',  # Nomes de arquivo DOC
                r'\.rar$',  # Nomes de arquivo RAR
                r'ilovepdf',  # Nome de arquivo específico
                r'merged',  # Nome de arquivo específico
                
                # Padrões gerais inválidos
                r'^$',  # Linha vazia
                r'^\s*$',  # Apenas espaços
            ]
            
            for pattern in invalid_patterns:
                if re.search(pattern, text):
                    return False
            
            # ACEITAR linhas que são claramente itens da licitação
            valid_patterns = [
                # Deve ter número + descrição + quantidade/valor
                r'\d+.*[a-zA-Z]{3,}.*\d+',  # Número + texto + número
                r'[a-zA-Z]{3,}.*\d+.*r\$',  # Texto + número + valor
                r'[a-zA-Z]{3,}.*\d+.*\d+',  # Texto + dois números
            ]
            
            for pattern in valid_patterns:
                if re.search(pattern, text):
                    # Verificação adicional: deve ter pelo menos uma palavra significativa
                    words = text.split()
                    significant_words = [w for w in words if len(w) > 3 and w.isalpha()]
                    
                    if len(significant_words) > 0:
                        return True
            
            # Se chegou até aqui, verificar se tem estrutura mínima de item
            # Deve ter pelo menos 10 caracteres e não ser apenas números/datas
            if len(text) >= 10 and not re.match(r'^[\d\s/:-]+$', text):
                return True
            
            return False
            
        except Exception as e:
            logger.warning(f"⚠️ Erro ao validar linha de item: {e}")
            return False
    
    async def extract_angular_row_data(self, row_element, index: int) -> Optional[Dict]:
        """Extrai dados de uma linha Angular datatable-body-row"""
        try:
            # Extrair todas as células da linha
            cells = await row_element.locator('datatable-body-cell').all()
            
            if len(cells) < 3:  # Precisa ter pelo menos 3 células (número, descrição, quantidade)
                return None
            
            # Extrair dados de cada célula usando spans Angular
            cell_data = []
            
            for cell in cells:
                try:
                    # Procurar por spans com conteúdo
                    spans = await cell.locator('span.ng-star-inserted').all()
                    
                    cell_text = ""
                    for span in spans:
                        span_text = await span.inner_text()
                        if span_text and span_text.strip():
                            cell_text = span_text.strip()
                            break
                    
                    # Se não encontrou span, pegar texto da célula diretamente
                    if not cell_text:
                        cell_text = await cell.inner_text()
                        cell_text = cell_text.strip()
                    
                    cell_data.append(cell_text)
                    
                except Exception as e:
                    logger.warning(f"⚠️ Erro ao extrair célula: {e}")
                    cell_data.append("")
            
            # VALIDAÇÃO ADICIONAL: Verificar se os dados extraídos são de item válido
            row_text = " ".join(cell_data)
            if not self.is_valid_items_row(row_text):
                logger.info(f"   ❌ Dados extraídos não são de item válido: {row_text[:50]}...")
                return None
            
            # Processar dados extraídos
            if len(cell_data) >= 3:
                numero = cell_data[0] if cell_data[0].isdigit() else str(index + 1)
                descricao = cell_data[1] if len(cell_data[1]) > 5 else f"Item {index + 1}"
                
                # Extrair quantidade
                quantidade = None
                for data in cell_data[2:]:
                    if data.isdigit():
                        quantidade = int(data)
                        break
                
                # Extrair valores (procurar por R$ ou números)
                valor_unitario = None
                valor_total = None
                
                for data in cell_data:
                    if 'R$' in data:
                        try:
                            valor_match = re.search(r'R\$\s*([\d.,]+)', data)
                            if valor_match:
                                valor_str = valor_match.group(1).replace('.', '').replace(',', '.')
                                if valor_unitario is None:
                                    valor_unitario = float(valor_str)
                                else:
                                    valor_total = float(valor_str)
                        except:
                            pass
                    elif data == "Sigiloso":
                        # Valores sigilosos
                        if valor_unitario is None:
                            valor_unitario = "Sigiloso"
                        else:
                            valor_total = "Sigiloso"
                
                return {
                    'numero': numero,
                    'descricao': descricao[:500] if descricao else f"Item {index + 1}",
                    'quantidade': quantidade,
                    'valor_unitario': valor_unitario,
                    'valor_total': valor_total,
                    'raw_data': cell_data,
                    'extraction_method': 'angular_items_tab_only'
                }
            
            return None
            
        except Exception as e:
            logger.warning(f"⚠️ Erro ao extrair dados da linha Angular: {e}")
            return None
    
    async def extract_items_from_visible_text(self, index: int) -> List[Dict]:
        """Fallback: Extrai itens do texto visível da aba Itens"""
        try:
            logger.info("📊 Fallback: extraindo itens do texto visível da aba Itens...")
            
            # Obter apenas o texto visível da página atual
            page_text = await self.page.inner_text('body')
            
            items = []
            lines = page_text.split('\n')
            
            # Procurar por seção de itens
            in_items_section = False
            
            for i, line in enumerate(lines):
                line = line.strip()
                
                # Detectar início da seção de itens
                if 'Número' in line and 'Descrição' in line and 'Quantidade' in line:
                    in_items_section = True
                    continue
                
                # Detectar fim da seção de itens
                if in_items_section and ('Arquivos' in line or 'Histórico' in line or 'Voltar' in line):
                    break
                
                # Processar linha se estiver na seção de itens
                if in_items_section and self.is_valid_items_row(line):
                    item = self.parse_item_row(line, len(items))
                    if item:
                        items.append(item)
                        logger.info(f"   📋 Item fallback {len(items)}: {item.get('descricao', 'N/A')[:50]}...")
            
            return items[:10]  # Limitar a 10 itens
            
        except Exception as e:
            logger.warning(f"⚠️ Erro ao extrair itens do texto visível: {e}")
            return []
    
    def parse_item_row(self, row_text: str, index: int) -> Optional[Dict]:
        """Extrai dados de uma linha de item"""
        try:
            # Dividir por tabs ou múltiplos espaços
            parts = re.split(r'\t+|\s{3,}', row_text.strip())
            
            # Filtrar partes vazias
            parts = [p.strip() for p in parts if p.strip()]
            
            if len(parts) < 3:
                return None
            
            # Tentar extrair campos
            numero = parts[0] if parts[0].isdigit() else str(index + 1)
            
            # Procurar descrição (geralmente a parte mais longa)
            descricao = ""
            for part in parts[1:]:
                if len(part) > len(descricao) and not re.match(r'^[\d\s,.$R]+$', part):
                    descricao = part
            
            # Extrair valores monetários
            valores = []
            for part in parts:
                if 'R$' in part:
                    valor_match = re.search(r'R\$\s*([\d.,]+)', part)
                    if valor_match:
                        try:
                            valor_str = valor_match.group(1).replace('.', '').replace(',', '.')
                            valores.append(float(valor_str))
                        except:
                            pass
            
            # Extrair quantidade
            quantidade = None
            for part in parts:
                if re.match(r'^\d+$', part.strip()) and part != numero:
                    quantidade = int(part)
                    break
            
            return {
                'numero': numero,
                'descricao': descricao[:500] if descricao else f"Item {index + 1}",
                'quantidade': quantidade,
                'valor_unitario': valores[0] if len(valores) > 0 else None,
                'valor_total': valores[1] if len(valores) > 1 else valores[0] if len(valores) > 0 else None,
                'raw_text': row_text,
                'extraction_method': 'text_fallback_items_only'
            }
            
        except Exception as e:
            logger.warning(f"⚠️ Erro ao processar linha de item: {e}")
            return None
    
    async def process_files_tab(self, index: int) -> Dict:
        """Processa APENAS a aba 'Arquivos' para downloads"""
        try:
            logger.info("🗂️ Processando aba 'Arquivos' para downloads...")
            
            # Screenshot antes de procurar aba Arquivos
            await self.page.screenshot(path=f"debug_10_antes_aba_arquivos_{index}.png")
            
            downloaded_files = []
            
            # Procurar pela aba "Arquivos"
            files_tab_selectors = [
                'li.tab-item:has-text("Arquivos")',
                'button:has-text("Arquivos")',
                '[class*="tab"]:has-text("Arquivos")',
                'li:has-text("Arquivos") button',
                '.tab-item:has-text("Arquivos") button'
            ]
            
            files_tab_found = False
            
            for selector in files_tab_selectors:
                try:
                    tab_button = self.page.locator(selector)
                    
                    if await tab_button.count() > 0:
                        logger.info(f"✅ Aba 'Arquivos' encontrada! (Seletor: {selector})")
                        
                        # Clicar na aba Arquivos
                        logger.info("🖱️ Clicando na aba 'Arquivos'...")
                        await tab_button.click()
                        await self.page.wait_for_timeout(3000)
                        
                        # Screenshot após clicar na aba
                        await self.page.screenshot(path=f"debug_11_aba_arquivos_ativa_{index}.png")
                        
                        files_tab_found = True
                        break
                        
                except Exception as e:
                    logger.warning(f"⚠️ Erro com seletor de aba Arquivos {selector}: {e}")
                    continue
            
            if files_tab_found:
                # Procurar por botões de download NA ABA ARQUIVOS
                downloaded_files = await self.find_and_download_files(index)
            else:
                logger.info("⚠️ Aba 'Arquivos' não encontrada, procurando downloads na página atual...")
                downloaded_files = await self.find_and_download_files(index)
            
            return {
                'files_tab_found': files_tab_found,
                'downloaded_files': downloaded_files,
                'downloads_count': len(downloaded_files)
            }
            
        except Exception as e:
            logger.error(f"❌ Erro ao processar aba de arquivos: {e}")
            return {'files_tab_found': False, 'downloaded_files': [], 'downloads_count': 0}
    
    async def find_and_download_files(self, index: int) -> List[Dict]:
        """Encontra e baixa arquivos da aba Arquivos"""
        try:
            logger.info("⬇️ Procurando arquivos para download na aba 'Arquivos'...")
            
            downloaded_files = []
            
            # Aguardar carregamento da aba
            await self.page.wait_for_timeout(3000)
            
            # Screenshot da aba arquivos
            await self.page.screenshot(path=f"debug_12_lista_arquivos_{index}.png")
            
            # Procurar por botões de download ESPECÍFICOS da aba Arquivos
            download_selectors = [
                'a.br-button.circle[href*="arquivos"]',  # Seletor específico do PNCP
                'a[aria-label="Fazer download"]',
                'a.br-button:has(i.fa-download)',
                'button:has(i.fa-download)',
                'a[href*="download"]',
                'a[href*="arquivo"]',
                'a[href*=".pdf"]',
                'a[href*=".doc"]',
                '.download-button',
                '[title*="download"]',
                '[title*="Download"]',
                '[title*="Baixar"]'
            ]
            
            for selector in download_selectors:
                try:
                    download_links = await self.page.locator(selector).all()
                    
                    if len(download_links) > 0:
                        logger.info(f"📎 Encontrados {len(download_links)} links de download (seletor: {selector})")
                        
                        for i, link in enumerate(download_links[:5]):  # Baixar até 5 arquivos
                            try:
                                # Extrair informações do link
                                href = await link.get_attribute('href')
                                title = await link.get_attribute('title') or ''
                                aria_label = await link.get_attribute('aria-label') or ''
                                
                                if href:
                                    # Construir URL completa
                                    if href.startswith('/'):
                                        full_url = f"{self.base_url}{href}"
                                    elif not href.startswith('http'):
                                        full_url = f"{self.base_url}/{href}"
                                    else:
                                        full_url = href
                                    
                                    # Tentar baixar o arquivo
                                    file_info = await self.download_file(link, full_url, index, i, title or aria_label)
                                    
                                    if file_info:
                                        downloaded_files.append(file_info)
                                        logger.info(f"✅ Arquivo {i+1} baixado: {file_info['filename']}")
                                
                            except Exception as e:
                                logger.warning(f"⚠️ Erro ao processar link {i+1}: {e}")
                                continue
                        
                        if downloaded_files:
                            break  # Se encontrou e baixou arquivos, parar
                            
                except Exception as e:
                    logger.warning(f"⚠️ Erro com seletor de download {selector}: {e}")
                    continue
            
            logger.info(f"📊 Total de arquivos baixados da aba 'Arquivos': {len(downloaded_files)}")
            return downloaded_files
            
        except Exception as e:
            logger.error(f"❌ Erro ao procurar arquivos na aba Arquivos: {e}")
            return []
    
    async def download_file(self, link_element, url: str, edital_index: int, file_index: int, description: str = "") -> Optional[Dict]:
        """Baixa um arquivo específico"""
        try:
            logger.info(f"⬇️ Baixando arquivo: {description[:30]}...")
            
            # Configurar listener para download
            download_info = None
            
            async def handle_download(download):
                nonlocal download_info
                download_info = download
                
                # Definir nome do arquivo
                suggested_name = download.suggested_filename or f"arquivo_{edital_index}_{file_index}.pdf"
                filename = f"edital_{edital_index}_{file_index}_{suggested_name}"
                filepath = os.path.join(self.download_dir, filename)
                
                # Salvar arquivo
                await download.save_as(filepath)
                logger.info(f"💾 Arquivo salvo: {filepath}")
                
                return {
                    'filename': filename,
                    'filepath': filepath,
                    'url': url,
                    'description': description,
                    'size': os.path.getsize(filepath) if os.path.exists(filepath) else 0,
                    'downloaded_at': datetime.now().isoformat()
                }
            
            # Registrar listener
            self.page.on("download", handle_download)
            
            try:
                # Clicar no link de download
                await link_element.click()
                await self.page.wait_for_timeout(5000)  # Aguardar download
                
                # Remover listener
                self.page.remove_listener("download", handle_download)
                
                if download_info:
                    return await handle_download(download_info)
                else:
                    logger.warning(f"⚠️ Download não iniciado para: {description}")
                    return None
                    
            except Exception as e:
                logger.warning(f"⚠️ Erro ao clicar no link de download: {e}")
                self.page.remove_listener("download", handle_download)
                return None
            
        except Exception as e:
            logger.warning(f"⚠️ Erro ao baixar arquivo: {e}")
            return None
    
    async def extract_detailed_info(self) -> Dict:
        """Extrai informações detalhadas da página do edital"""
        try:
            logger.info("📋 Extraindo informações detalhadas...")
            
            await self.page.wait_for_timeout(2000)
            
            page_text = await self.page.inner_text('body')
            
            detailed_description = page_text[:2000]
            
            # Extrair valor total estimado
            valor_total = self.extract_valor_total(page_text)
            
            # Extrair informações específicas
            objeto_detalhado = self.extract_objeto_detalhado(page_text)
            valor_estimado = self.extract_valor_estimado(page_text)
            prazo = self.extract_prazo(page_text)
            
            return {
                'has_details': True,
                'detailed_description': detailed_description,
                'objeto_detalhado': objeto_detalhado,
                'valor_estimado_detalhado': valor_estimado,
                'valor_total_estimado': valor_total,
                'prazo': prazo
            }
            
        except Exception as e:
            logger.warning(f"⚠️ Erro ao extrair detalhes: {e}")
            return {'has_details': False}
    
    async def try_access_contratacao(self, index: int) -> Dict:
        """Tenta acessar a contratação"""
        try:
            logger.info("🔍 Procurando botão 'Acessar contratação'...")
            
            access_selectors = [
                "button:has-text('Acessar contratação')",
                "a:has-text('Acessar contratação')",
                "button:has-text('Acessar Contratação')",
                "a:has-text('Acessar Contratação')",
                "[title*='Acessar contratação']",
                "[title*='Acessar Contratação']"
            ]
            
            access_button = None
            used_selector = None
            
            for selector in access_selectors:
                try:
                    button = self.page.locator(selector)
                    if await button.count() > 0:
                        access_button = button
                        used_selector = selector
                        break
                except:
                    continue
            
            if access_button and await access_button.count() > 0:
                logger.info(f"✅ Botão 'Acessar contratação' encontrado! (Seletor: {used_selector})")
                
                await self.page.screenshot(path=f"debug_13_antes_acesso_{index}.png")
                
                await access_button.click()
                await self.page.wait_for_timeout(4000)
                
                await self.page.screenshot(path=f"debug_14_apos_acesso_{index}.png")
                
                logger.info("📂 Acessou contratação com sucesso!")
                
                return {
                    'has_access_button': True,
                    'accessed_contratacao': True,
                    'access_button_selector': used_selector
                }
                
            else:
                logger.info("⚠️ Botão 'Acessar contratação' não encontrado")
                return {
                    'has_access_button': False,
                    'accessed_contratacao': False
                }
                
        except Exception as e:
            logger.warning(f"⚠️ Erro ao tentar acessar contratação: {e}")
            return {
                'has_access_button': False,
                'accessed_contratacao': False
            }
    
    def extract_valor_total(self, text: str) -> Optional[float]:
        """Extrai valor total estimado"""
        try:
            # Procurar por "VALOR TOTAL ESTIMADO"
            pattern = r'VALOR TOTAL ESTIMADO.*?R\$\s*([\d.,]+)'
            match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
            
            if match:
                valor_str = match.group(1).replace('.', '').replace(',', '.')
                return float(valor_str)
            
            return None
        except:
            return None
    
    # Métodos de extração básica (mantidos do código anterior)
    def extract_basic_info(self, card_text: str, index: int) -> Dict:
        """Extrai informações básicas do card"""
        
        title = self.safe_extract_title(card_text)
        organization = self.safe_extract_organization(card_text)
        municipality = self.safe_extract_municipality(card_text)
        modality = self.safe_extract_modality(card_text)
        value = self.safe_extract_value(card_text)
        pub_date = self.safe_extract_date(card_text)
        pncp_id = self.safe_extract_pncp_id(card_text)
        objeto = self.safe_extract_objeto(card_text)
        
        return {
            'id': f"PNCP-ITEMS-ONLY-{datetime.now().strftime('%Y%m%d')}-{index:03d}",
            'pncp_id': pncp_id,
            'title': title or f"Edital #{index+1}",
            'description': card_text[:500] + "..." if len(card_text) > 500 else card_text,
            'organization_name': organization,
            'municipality_name': municipality,
            'state_code': 'SP',
            'modality': modality,
            'estimated_value': value,
            'publication_date': pub_date,
            'objeto': objeto,
            'status': 'Publicado',
            'source_url': self.page.url,
            'data_source': 'PNCP_SCRAPING_ITEMS_ONLY',
            'raw_text': card_text,
            'scraped_at': datetime.now().isoformat(),
            'has_details': False,
            'has_access_button': False
        }
    
    def safe_extract_title(self, text: str) -> Optional[str]:
        """Extrai título de forma segura"""
        try:
            lines = text.split('\n')
            for line in lines:
                line = line.strip()
                if 'Edital nº' in line or 'Aviso' in line:
                    return line[:200]
            
            for line in lines:
                line = line.strip()
                if len(line) > 15:
                    return line[:200]
            
            return None
        except:
            return None
    
    def safe_extract_organization(self, text: str) -> Optional[str]:
        """Extrai organização de forma segura"""
        try:
            if 'Órgão:' in text:
                parts = text.split('Órgão:')
                if len(parts) > 1:
                    org_part = parts[1].split('\n')[0].strip()
                    return org_part[:200] if org_part else None
            return None
        except:
            return None
    
    def safe_extract_municipality(self, text: str) -> Optional[str]:
        """Extrai município de forma segura"""
        try:
            if 'Local:' in text:
                parts = text.split('Local:')
                if len(parts) > 1:
                    local_part = parts[1].split('\n')[0].strip()
                    if '/' in local_part:
                        local_part = local_part.split('/')[0].strip()
                    return local_part[:100] if local_part else None
            return None
        except:
            return None
    
    def safe_extract_modality(self, text: str) -> str:
        """Extrai modalidade de forma segura"""
        try:
            if 'Modalidade da Contratação:' in text:
                parts = text.split('Modalidade da Contratação:')
                if len(parts) > 1:
                    mod_part = parts[1].split('\n')[0].strip()
                    return mod_part[:100] if mod_part else 'Não informado'
            return 'Não informado'
        except:
            return 'Não informado'
    
    def safe_extract_value(self, text: str) -> Optional[float]:
        """Extrai valor de forma segura"""
        try:
            pattern = r'R\$\s*([\d\.]+,\d{2})'
            matches = re.findall(pattern, text)
            
            for match in matches:
                try:
                    value_str = match.replace('.', '').replace(',', '.')
                    return float(value_str)
                except:
                    continue
            
            return None
        except:
            return None
    
    def safe_extract_date(self, text: str) -> str:
        """Extrai data de forma segura"""
        try:
            if 'Última Atualização:' in text:
                parts = text.split('Última Atualização:')
                if len(parts) > 1:
                    date_part = parts[1].split('\n')[0].strip()
                    pattern = r'(\d{1,2}/\d{1,2}/\d{4})'
                    match = re.search(pattern, date_part)
                    if match:
                        try:
                            date_obj = datetime.strptime(match.group(1), '%d/%m/%Y')
                            return date_obj.date().isoformat()
                        except:
                            pass
            
            return date.today().isoformat()
        except:
            return date.today().isoformat()
    
    def safe_extract_pncp_id(self, text: str) -> Optional[str]:
        """Extrai ID PNCP de forma segura"""
        try:
            if 'Id contratação PNCP:' in text:
                parts = text.split('Id contratação PNCP:')
                if len(parts) > 1:
                    id_part = parts[1].split('\n')[0].strip()
                    return id_part[:100] if id_part else None
            return None
        except:
            return None
    
    def safe_extract_objeto(self, text: str) -> Optional[str]:
        """Extrai objeto de forma segura"""
        try:
            if 'Objeto:' in text:
                parts = text.split('Objeto:')
                if len(parts) > 1:
                    obj_part = parts[1].split('\n')[0].strip()
                    return obj_part[:500] if obj_part else None
            return None
        except:
            return None
    
    def extract_objeto_detalhado(self, text: str) -> Optional[str]:
        """Extrai objeto detalhado da página"""
        try:
            if 'Objeto:' in text:
                parts = text.split('Objeto:')
                if len(parts) > 1:
                    obj_part = parts[1].split('\n')[0].strip()
                    return obj_part[:1000] if obj_part else None
            return None
        except:
            return None
    
    def extract_valor_estimado(self, text: str) -> Optional[str]:
        """Extrai valor estimado detalhado"""
        try:
            if 'Valor Estimado:' in text:
                parts = text.split('Valor Estimado:')
                if len(parts) > 1:
                    val_part = parts[1].split('\n')[0].strip()
                    return val_part[:200] if val_part else None
            return None
        except:
            return None
    
    def extract_prazo(self, text: str) -> Optional[str]:
        """Extrai prazo da contratação"""
        try:
            if 'Prazo:' in text:
                parts = text.split('Prazo:')
                if len(parts) > 1:
                    prazo_part = parts[1].split('\n')[0].strip()
                    return prazo_part[:200] if prazo_part else None
            return None
        except:
            return None
    
    async def scrape_editais(self, uf: str, limit: int = None) -> List[Dict]:
        """Função principal de scraping com extração APENAS da aba Itens"""
        logger.info(f"🚀 Iniciando scraping com extração APENAS da aba 'Itens' para UF: {uf}")
        logger.info("📊 Aba 'Itens' → APENAS dados da tabela visível")
        logger.info("🗂️ Aba 'Arquivos' → Downloads de arquivos")
        
        editais = []
        
        try:
            if not await self.navigate_and_filter(uf):
                logger.error("❌ Falha ao navegar e filtrar")
                return []
            
            total_editais = await self.get_editais_count()
            
            if total_editais == 0:
                logger.warning("⚠️ Nenhum edital encontrado")
                return []
            
            # Processar TODOS os editais se limit não for especificado
            if limit is None:
                editais_to_process = total_editais
                logger.info(f"📊 Processando TODOS os {total_editais} editais encontrados")
            else:
                editais_to_process = min(total_editais, limit)
                logger.info(f"📊 Processando {editais_to_process} de {total_editais} editais (limitado)")
            
            for i in range(editais_to_process):
                logger.info(f"\n🔄 Processando edital {i+1}/{editais_to_process}")
                
                edital_info = await self.process_edital(i)
                
                if edital_info:
                    editais.append(edital_info)
                    logger.info(f"✅ Edital {i+1} adicionado à lista")
                else:
                    logger.warning(f"⚠️ Falha ao processar edital {i+1}")
                
                await self.page.wait_for_timeout(2000)
            
            logger.info(f"🎉 Scraping com extração APENAS da aba 'Itens' concluído! {len(editais)} editais coletados")
            return editais
            
        except Exception as e:
            logger.error(f"❌ Erro no scraping: {e}")
            return editais

async def save_to_database(editais: List[Dict], db_path: str = 'src/database/app.db'):
    """Salva editais no banco de dados"""
    logger.info(f"💾 Salvando {len(editais)} editais no banco...")
    
    try:
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Criar tabela se não existir
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tenders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pncp_id VARCHAR(50) UNIQUE,
                title VARCHAR(500) NOT NULL,
                description TEXT,
                organization_name VARCHAR(200),
                municipality_name VARCHAR(100),
                state_code VARCHAR(2),
                publication_date DATE,
                update_date DATETIME,
                status VARCHAR(50),
                modality VARCHAR(100),
                estimated_value FLOAT,
                source_url VARCHAR(500),
                data_source VARCHAR(20),
                created_at DATETIME NOT NULL
            )
        ''')
        
        # Limpar dados antigos
        cursor.execute("DELETE FROM tenders WHERE data_source = 'PNCP_SCRAPING_ITEMS_ONLY'")
        
        saved = 0
        for edital in editais:
            try:
                cursor.execute('''
                    INSERT INTO tenders (
                        pncp_id, title, description, organization_name,
                        municipality_name, state_code, publication_date,
                        update_date, status, modality, estimated_value,
                        source_url, data_source, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    edital.get('pncp_id'), edital.get('title'), edital.get('description'),
                    edital.get('organization_name'), edital.get('municipality_name'),
                    edital.get('state_code'), edital.get('publication_date'),
                    datetime.now().isoformat(), edital.get('status'),
                    edital.get('modality'), edital.get('estimated_value'),
                    edital.get('source_url'), 'PNCP_SCRAPING_ITEMS_ONLY',
                    datetime.now().isoformat()
                ))
                saved += 1
            except sqlite3.IntegrityError:
                pass
            except Exception as e:
                logger.error(f"❌ Erro ao salvar edital: {e}")
        
        conn.commit()
        conn.close()
        
        logger.info(f"✅ Salvos {saved} editais no banco!")
        return saved
        
    except Exception as e:
        logger.error(f"❌ Erro ao salvar no banco: {e}")
        return 0

async def main():
    """Função principal"""
    logger.info("🎯 Iniciando PNCP Scraper - APENAS aba 'Itens'")
    logger.info("📊 Extração RESTRITA à aba 'Itens' ativa")
    logger.info("🗂️ Downloads da aba 'Arquivos'")
    
    # Configurações
    uf = "SP"
    limit = 50  # Processar apenas 3 editais para teste (mude para None para processar todos)
    
    all_editais = []
    
    async with PNCPScraperItemsOnly(headless=False) as scraper:
        logger.info(f"\n{'='*60}")
        logger.info(f"🎯 PROCESSANDO EDITAIS - APENAS ABA 'ITENS' - UF: {uf}")
        logger.info(f"{'='*60}")
        
        editais = await scraper.scrape_editais(uf, limit)
        
        if editais:
            all_editais.extend(editais)
            logger.info(f"🎉 SUCESSO! {len(editais)} editais coletados")
            
            # Mostrar resumo detalhado
            for i, edital in enumerate(editais):
                logger.info(f"\n--- EDITAL {i+1} ---")
                logger.info(f"📋 Título: {edital['title']}")
                logger.info(f"🆔 PNCP ID: {edital.get('pncp_id', 'N/A')}")
                logger.info(f"🏢 Organização: {edital.get('organization_name', 'N/A')}")
                logger.info(f"🏙️ Município: {edital.get('municipality_name', 'N/A')}")
                logger.info(f"📝 Modalidade: {edital.get('modality', 'N/A')}")
                logger.info(f"💰 Valor Total: R$ {edital.get('valor_total_estimado', 'N/A')}")
                logger.info(f"📊 Aba Itens encontrada: {edital.get('items_tab_found', False)}")
                logger.info(f"📊 Itens extraídos APENAS da aba 'Itens': {edital.get('items_count', 0)}")
                logger.info(f"🗂️ Aba Arquivos encontrada: {edital.get('files_tab_found', False)}")
                logger.info(f"⬇️ Arquivos baixados: {edital.get('downloads_count', 0)}")
                
                # Mostrar itens se houver (APENAS da aba Itens)
                if edital.get('items'):
                    logger.info(f"   📊 ITENS DA ABA 'ITENS' (APENAS):")
                    for j, item in enumerate(edital['items']):
                        logger.info(f"      {j+1}. {item.get('descricao', 'N/A')[:50]}...")
                        logger.info(f"         Qtd: {item.get('quantidade', 'N/A')} | Valor: {item.get('valor_total', 'N/A')}")
                        logger.info(f"         Método: {item.get('extraction_method', 'N/A')}")
                
                # Mostrar arquivos baixados se houver
                if edital.get('downloaded_files'):
                    logger.info(f"   🗂️ ARQUIVOS DA ABA 'ARQUIVOS':")
                    for j, file_info in enumerate(edital['downloaded_files']):
                        logger.info(f"      {j+1}. {file_info['filename']} ({file_info.get('size', 0)} bytes)")
                
        else:
            logger.warning(f"⚠️ Nenhum edital coletado para {uf}")
    
    # Salvar resultados
    if all_editais:
        logger.info(f"\n{'='*60}")
        logger.info(f"📊 RESUMO FINAL - APENAS ABA 'ITENS'")
        logger.info(f"{'='*60}")
        logger.info(f"📈 Total de editais coletados: {len(all_editais)}")
        
        # Salvar JSON
        output_file = f"editais_items_only_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(all_editais, f, ensure_ascii=False, indent=2, default=str)
        
        logger.info(f"💾 Arquivo JSON salvo: {output_file}")
        
        # Salvar no banco
        saved = await save_to_database(all_editais)
        logger.info(f"🗄️ Banco de dados atualizado: {saved} editais")
        
        # Estatísticas finais
        total_items = sum(edital.get('items_count', 0) for edital in all_editais)
        total_downloads = sum(edital.get('downloads_count', 0) for edital in all_editais)
        with_items_tab = sum(1 for e in all_editais if e.get('items_tab_found'))
        with_files_tab = sum(1 for e in all_editais if e.get('files_tab_found'))
        with_items = sum(1 for e in all_editais if e.get('items_count', 0) > 0)
        
        logger.info(f"\n📊 Estatísticas FINAIS - APENAS ABA 'ITENS':")
        logger.info(f"   📋 Total de editais: {len(all_editais)}")
        logger.info(f"   📊 Abas 'Itens' encontradas: {with_items_tab}")
        logger.info(f"   📊 Editais com itens extraídos (APENAS aba Itens): {with_items}")
        logger.info(f"   📊 Total de itens extraídos (APENAS aba Itens): {total_items}")
        logger.info(f"   🗂️ Abas 'Arquivos' encontradas: {with_files_tab}")
        logger.info(f"   ⬇️ Total de arquivos baixados: {total_downloads}")
        
        logger.info(f"\n📸 Arquivos de debug gerados:")
        logger.info(f"   🖼️ debug_*.png (screenshots de cada etapa)")
        logger.info(f"   📁 downloads/ (arquivos baixados)")
        
    else:
        logger.warning("❌ Nenhum edital foi coletado")
        logger.info("🔍 Verifique os arquivos de debug para análise")

if __name__ == "__main__":
    asyncio.run(main())

