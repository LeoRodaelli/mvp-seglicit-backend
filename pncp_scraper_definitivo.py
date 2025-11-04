#!/usr/bin/env python3
"""
PNCP Scraper Definitivo - Extração Correta de Todas as Informações
Baseado no pncp_scraper_items_only.py original que funciona
"""

import asyncio
import os
import json
import psycopg2
from datetime import datetime, date
from typing import List, Dict, Optional
import re
import logging
from pathlib import Path
from dotenv import load_dotenv

# Carregar variáveis de ambiente
load_dotenv()

try:
    from playwright.async_api import async_playwright, Page, Browser
except ImportError:
    print("ERRO: Playwright não instalado. Execute: pip install playwright")
    print("   Depois execute: playwright install")
    exit(1)

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pncp_scraper_definitivo.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class PNCPScraperDefinitivo:
    """Scraper definitivo com extração correta baseada no original"""

    def __init__(self, headless: bool = True, save_screenshots: bool = False, download_files: bool = True):
        self.headless = headless
        self.save_screenshots = save_screenshots
        self.download_files = download_files
        self.base_url = "https://pncp.gov.br"
        self.editais_url = f"{self.base_url}/app/editais?pagina=1"
        self.browser: Optional[Browser] = None
        self.page: Optional[Page] = None
        self.download_dir = "downloads"
        
        # Estados brasileiros para processar
        self.estados = [
            "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", 
            "MA", "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI", 
            "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO"
        ]
        
        # Estados prioritários (maiores)
        self.estados_prioritarios = ["SP", "RJ", "MG", "RS", "PR", "SC", "BA", "GO", "DF"]

        # Criar diretório de downloads
        if self.download_files or self.save_screenshots:
            os.makedirs(self.download_dir, exist_ok=True)

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def start(self):
        """Inicializa o navegador"""
        logger.info("🧭 Iniciando browser definitivo...")

        self.playwright = await async_playwright().start()
        
        browser_args = [
            '--no-sandbox',
            '--disable-dev-shm-usage',
            '--disable-gpu',
            '--disable-extensions'
        ]

        self.browser = await self.playwright.chromium.launch(
            headless=self.headless,
            args=browser_args
        )

        context = await self.browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={'width': 1920, 'height': 1080},
            accept_downloads=True
        )

        self.page = await context.new_page()
        self.page.set_default_timeout(30000)

        logger.info("✅ Navegador definitivo iniciado com sucesso")

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
            logger.info(f"🌐 Acessando página de editais para UF: {uf}...")
            
            url_with_filter = f"{self.base_url}/app/editais?pagina=1&ufs={uf}&q=&status=recebendo_proposta"
            await self.page.goto(url_with_filter, timeout=30000)
            await self.page.wait_for_timeout(3000)

            if self.save_screenshots:
                await self.page.screenshot(path=f"debug_01_inicial_{uf}.png")

            try:
                await self.page.wait_for_selector("a.br-item", timeout=10000)
                logger.info(f"✅ Página {uf} carregada com sucesso")
                return True
            except:
                alternative_selectors = ['a[href*="/editais/"]', '.br-item']
                for selector in alternative_selectors:
                    try:
                        await self.page.wait_for_selector(selector, timeout=5000)
                        logger.info(f"✅ Página {uf} carregada com seletor alternativo: {selector}")
                        return True
                    except:
                        continue
                
                logger.warning(f"⚠️ Nenhum edital encontrado para {uf}")
                return False

        except Exception as e:
            logger.error(f"❌ Erro ao navegar e filtrar {uf}: {e}")
            return False

    async def get_editais_count(self) -> int:
        """Conta quantos editais estão disponíveis"""
        try:
            await self.page.wait_for_timeout(1000)
            cards = await self.page.locator("a.br-item").all()
            count = len(cards)

            if count == 0:
                alternative_selectors = ['a[href*="/editais/"]', '.br-item']
                for selector in alternative_selectors:
                    try:
                        cards = await self.page.locator(selector).all()
                        if len(cards) > 0:
                            return len(cards)
                    except:
                        continue

            return count

        except Exception as e:
            logger.error(f"❌ Erro ao contar editais: {e}")
            return 0

    async def click_next_page(self) -> bool:
        """Clica no botão 'Página seguinte'"""
        try:
            next_button_selectors = [
                'button[data-next-page="data-next-page"]',
                'button[aria-label="Página seguinte"]',
                'button.br-button.circle:has(i.fa-angle-right)',
                'button:has(i.fas.fa-angle-right)'
            ]

            for selector in next_button_selectors:
                try:
                    next_button = self.page.locator(selector)

                    if await next_button.count() > 0:
                        is_disabled = await next_button.first.is_disabled()
                        is_visible = await next_button.first.is_visible()

                        if not is_disabled and is_visible:
                            await next_button.first.click()
                            await self.page.wait_for_timeout(3000)
                            
                            try:
                                await self.page.wait_for_selector("a.br-item", timeout=10000)
                            except:
                                pass
                            
                            return True

                except Exception as e:
                    continue

            return False

        except Exception as e:
            logger.error(f"❌ Erro ao clicar próxima página: {e}")
            return False

    async def process_edital(self, index: int, uf: str) -> Optional[Dict]:
        """Processa um edital específico com extração completa"""
        try:
            logger.info(f"\\n📄 PROCESSANDO EDITAL {index + 1}")
            
            await self.page.wait_for_timeout(1000)

            # Obter todos os cards
            cards = await self.page.locator("a.br-item").all()

            if len(cards) == 0:
                alternative_selectors = ['a[href*="/editais/"]']
                for selector in alternative_selectors:
                    try:
                        cards = await self.page.locator(selector).all()
                        if len(cards) > 0:
                            break
                    except:
                        continue

            if index >= len(cards):
                return None

            # Extrair informações básicas do card
            card = cards[index]
            card_text = await card.inner_text()
            href = await card.get_attribute('href')

            logger.info(f"📋 Título: {card_text.split(chr(10))[0][:50]}...")

            # Criar estrutura básica do edital
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            edital_id = f"PNCP-DEFINITIVO-{timestamp}-{uf}-{index:03d}"

            # Extrair título (primeira linha)
            lines = card_text.strip().split('\\n')
            title = lines[0] if lines else "Título não encontrado"

            edital_info = {
                "id": edital_id,
                "title": title,
                "description": card_text,
                "state_code": uf,
                "status": "Publicado",
                "source_url": f"https://pncp.gov.br/app/editais?ufs={uf}",
                "data_source": "PNCP_SCRAPING_DEFINITIVO",
                "raw_text": card_text,
                "scraped_at": datetime.now().isoformat(),
                "publication_date": datetime.now().strftime("%Y-%m-%d"),
                "has_details": False,
                "items": [],
                "downloaded_files": []
            }

            # Construir detail_url correta
            if href:
                edital_info['edital_href'] = href
                edital_info['detail_url'] = href if href.startswith('http') else f"{self.base_url}{href}"

            if self.save_screenshots:
                await self.page.screenshot(path=f"debug_card_{uf}_{index}.png")

            # Navegar para página do edital para extração completa
            if href:
                full_url = edital_info['detail_url']
                logger.info(f"🔗 Navegando para: {full_url}")
                
                await self.page.goto(full_url, timeout=30000)
                await self.page.wait_for_timeout(3000)

                # Extrair dados detalhados, itens e arquivos
                detailed_info = await self.extract_complete_info_corrected(index, uf)
                edital_info.update(detailed_info)

                # Voltar para lista de editais
                await self.page.go_back()
                await self.page.wait_for_timeout(2000)

            return edital_info

        except Exception as e:
            logger.error(f"❌ Erro ao processar edital {index}: {e}")
            return None

    async def extract_complete_info_corrected(self, index: int, uf: str) -> Dict:
        """Extrai informações completas CORRIGIDAS da página detalhada"""
        try:
            logger.info("📊 Extraindo informações completas corrigidas...")
            
            await self.page.wait_for_timeout(3000)

            # Extrair texto completo da página detalhada
            page_text = await self.page.inner_text('body')

            if self.save_screenshots:
                await self.page.screenshot(path=f"debug_detail_{uf}_{index}.png")

            # Extrair informações básicas CORRIGIDAS da página detalhada
            basic_info = self.extract_basic_info_from_detailed_page(page_text)

            # Extrair valor total estimado
            valor_total = self.extract_valor_total_from_detailed_page(page_text)

            # Extrair itens da aba "Itens"
            items_info = await self.extract_items_complete(index, uf)

            # Extrair/baixar arquivos da aba "Arquivos"
            files_info = await self.extract_files_complete(index, uf)

            # Combinar todas as informações
            result = {
                "detailed_description": page_text[:2000],
                "valor_total_estimado": valor_total,
                "items": items_info['items'],
                "items_count": len(items_info['items']),
                "items_tab_found": items_info['items_tab_found'],
                "downloaded_files": files_info['files'],
                "downloads_count": len(files_info['files']),
                "files_tab_found": files_info['files_tab_found'],
                "has_details": True
            }

            # Adicionar informações básicas extraídas
            result.update(basic_info)

            return result

        except Exception as e:
            logger.error(f"❌ Erro ao extrair informações completas: {e}")
            return {
                "detailed_description": "",
                "pncp_id": "",
                "organization_name": "",
                "municipality_name": "",
                "modality": "",
                "objeto": "",
                "estimated_value": None,
                "valor_total_estimado": None,
                "items": [],
                "items_count": 0,
                "items_tab_found": False,
                "downloaded_files": [],
                "downloads_count": 0,
                "files_tab_found": False,
                "has_details": False
            }

    def extract_basic_info_from_detailed_page(self, page_text: str) -> Dict:
        """Extrai informações básicas da página detalhada - MÉTODO CORRIGIDO"""
        logger.info("🔍 Extraindo informações básicas da página detalhada...")

        # Extrair PNCP ID
        pncp_id = ""
        pncp_patterns = [
            r'Id contratação PNCP:\\s*([^\\n\\r]+)',
            r'PNCP:\\s*([^\\n\\r]+)',
            r'Id PNCP:\\s*([^\\n\\r]+)'
        ]
        
        for pattern in pncp_patterns:
            match = re.search(pattern, page_text, re.IGNORECASE)
            if match:
                pncp_id = match.group(1).strip()
                if pncp_id and len(pncp_id) > 5:
                    break

        # Extrair nome da organização
        organization_name = ""
        org_patterns = [
            r'Órgão:\\s*([^\\n\\r]+)',
            r'Orgão:\\s*([^\\n\\r]+)',
            r'Entidade:\\s*([^\\n\\r]+)'
        ]
        
        for pattern in org_patterns:
            match = re.search(pattern, page_text, re.IGNORECASE)
            if match:
                organization_name = match.group(1).strip()
                if organization_name and len(organization_name) > 3:
                    break

        # Extrair município
        municipality_name = ""
        mun_patterns = [
            r'Local:\\s*([^/\\n\\r]+)',
            r'Município:\\s*([^/\\n\\r]+)',
            r'Cidade:\\s*([^/\\n\\r]+)'
        ]
        
        for pattern in mun_patterns:
            match = re.search(pattern, page_text, re.IGNORECASE)
            if match:
                municipality_name = match.group(1).strip()
                # Remover "/SP" ou similar do final
                municipality_name = re.sub(r'/[A-Z]{2}$', '', municipality_name)
                if municipality_name and len(municipality_name) > 2:
                    break

        # Extrair modalidade
        modality = ""
        mod_patterns = [
            r'Modalidade da contratação:\\s*([^\\n\\r]+)',
            r'Modalidade:\\s*([^\\n\\r]+)',
            r'Tipo:\\s*([^\\n\\r]+)'
        ]
        
        for pattern in mod_patterns:
            match = re.search(pattern, page_text, re.IGNORECASE)
            if match:
                modality = match.group(1).strip()
                if modality and len(modality) > 3:
                    break

        # Extrair objeto
        objeto = ""
        obj_patterns = [
            r'Objeto:\\s*([^\\n\\r]+(?:\\n[^\\n\\r]+)*?)(?=\\n\\n|\\nInformação|\\nVALOR|$)',
            r'Objeto:\\s*([^\\n\\r]+)',
            r'Descrição:\\s*([^\\n\\r]+)'
        ]
        
        for pattern in obj_patterns:
            match = re.search(pattern, page_text, re.IGNORECASE | re.DOTALL)
            if match:
                objeto = match.group(1).strip()
                # Limpar quebras de linha extras
                objeto = re.sub(r'\\n+', ' ', objeto)
                if objeto and len(objeto) > 10:
                    break

        # Log dos dados extraídos
        logger.info(f"📊 Dados básicos extraídos da página detalhada:")
        logger.info(f"   PNCP ID: {pncp_id}")
        logger.info(f"   Órgão: {organization_name}")
        logger.info(f"   Município: {municipality_name}")
        logger.info(f"   Modalidade: {modality}")
        logger.info(f"   Objeto: {objeto[:50]}..." if objeto else "   Objeto: (vazio)")

        return {
            "pncp_id": pncp_id,
            "organization_name": organization_name,
            "municipality_name": municipality_name,
            "modality": modality,
            "objeto": objeto,
            "estimated_value": None  # Será preenchido pelo valor_total_estimado
        }

    def extract_valor_total_from_detailed_page(self, page_text: str) -> Optional[float]:
        """Extrai valor total da página detalhada"""
        patterns = [
            r'VALOR TOTAL ESTIMADO DA COMPRA\\s*R\\$\\s*([\\d.,]+)',
            r'VALOR TOTAL ESTIMADO\\s*R\\$\\s*([\\d.,]+)',
            r'Valor total.*?R\\$\\s*([\\d.,]+)',
            r'Total estimado.*?R\\$\\s*([\\d.,]+)'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, page_text, re.IGNORECASE | re.DOTALL)
            if match:
                try:
                    valor_str = match.group(1)
                    # Tratar formato brasileiro: 1.234.567,89
                    if ',' in valor_str:
                        parts = valor_str.split(',')
                        if len(parts) == 2:
                            parte_inteira = parts[0].replace('.', '')
                            parte_decimal = parts[1]
                            valor_final = f"{parte_inteira}.{parte_decimal}"
                        else:
                            valor_final = valor_str.replace('.', '').replace(',', '.')
                    else:
                        valor_final = valor_str.replace('.', '')
                    
                    valor_float = float(valor_final)
                    logger.info(f"💰 Valor total extraído: R$ {valor_float:,.2f}")
                    return valor_float
                except Exception as e:
                    logger.warning(f"⚠️ Erro ao converter valor '{valor_str}': {e}")
                    continue
        
        logger.warning("⚠️ Valor total não encontrado")
        return None

    async def extract_items_complete(self, index: int, uf: str) -> Dict:
        """Extrai itens completos da aba 'Itens'"""
        try:
            logger.info("📊 Extraindo itens da aba 'Itens'...")

            # Tentar ativar aba Itens
            items_tab_found = await self.ensure_items_tab_active()

            if not items_tab_found:
                logger.warning("⚠️ Aba 'Itens' não encontrada")
                return {'items_tab_found': False, 'items': []}

            await self.page.wait_for_timeout(3000)

            if self.save_screenshots:
                await self.page.screenshot(path=f"debug_items_{uf}_{index}.png")

            # Extrair itens da tabela ativa
            items = await self.extract_items_from_active_tab()

            logger.info(f"📊 Extraídos {len(items)} itens da aba 'Itens'")

            return {
                'items_tab_found': True,
                'items': items
            }

        except Exception as e:
            logger.error(f"❌ Erro ao extrair itens: {e}")
            return {'items_tab_found': False, 'items': []}

    async def ensure_items_tab_active(self) -> bool:
        """Garante que a aba 'Itens' está ativa"""
        try:
            logger.info("🎯 Ativando aba 'Itens'...")

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

                        try:
                            tab_element = await tab_button.first.element_handle()
                            if tab_element:
                                class_attr = await tab_element.get_attribute('class')
                                if 'is-active' in (class_attr or ''):
                                    logger.info("📊 Aba 'Itens' já está ativa")
                                else:
                                    logger.info("🖱️ Clicando na aba 'Itens'...")
                                    await tab_button.first.click()
                                    await self.page.wait_for_timeout(3000)
                                    logger.info("✅ Aba 'Itens' ativada")
                        except:
                            await tab_button.first.click()
                            await self.page.wait_for_timeout(3000)

                        return True

                except Exception as e:
                    logger.warning(f"⚠️ Erro com seletor de aba Itens {selector}: {e}")
                    continue

            logger.warning("⚠️ Aba 'Itens' não encontrada")
            return False

        except Exception as e:
            logger.error(f"❌ Erro ao ativar aba Itens: {e}")
            return False

    async def extract_items_from_active_tab(self) -> List[Dict]:
        """Extrai itens da tabela ativa da aba 'Itens'"""
        try:
            items = []

            # Seletores para linhas da tabela de itens
            row_selectors = [
                'div[role="tabpanel"]:not([hidden]) datatable-body-row',
                'div[aria-hidden="false"] datatable-body-row',
                'div.tab-content:not(.d-none) datatable-body-row',
                'div.active datatable-body-row',
                'datatable-body-row:visible',
                'datatable-body-row'
            ]

            for selector in row_selectors:
                try:
                    logger.info(f"🔍 Testando seletor de linhas: {selector}")

                    rows = await self.page.locator(selector).all()

                    if len(rows) > 0:
                        logger.info(f"📊 Encontradas {len(rows)} linhas com seletor: {selector}")

                        # Validar e processar linhas
                        valid_rows = []
                        for i, row in enumerate(rows):
                            try:
                                is_visible = await row.is_visible()
                                if not is_visible:
                                    continue

                                row_text = await row.inner_text()
                                if self.is_valid_items_row(row_text):
                                    valid_rows.append(row)

                            except Exception as e:
                                continue

                        # Processar linhas válidas
                        if valid_rows:
                            logger.info(f"📊 Processando {len(valid_rows)} linhas válidas")

                            for i, row in enumerate(valid_rows[:10]):  # Limitar a 10 itens
                                try:
                                    item_data = await self.extract_angular_row_data_corrected(row, i)
                                    if item_data:
                                        items.append(item_data)
                                        logger.info(f"   📋 Item {i+1}: {item_data.get('descricao', 'N/A')[:50]}...")

                                except Exception as e:
                                    logger.warning(f"⚠️ Erro ao processar linha {i}: {e}")
                                    continue

                            break

                except Exception as e:
                    logger.warning(f"⚠️ Erro com seletor {selector}: {e}")
                    continue

            # Se não encontrou com seletores específicos, tentar fallback
            if not items:
                logger.info("📊 Tentando fallback: extrair do texto visível...")
                items = await self.extract_items_from_visible_text()

            return items

        except Exception as e:
            logger.error(f"❌ Erro ao extrair itens da tabela ativa: {e}")
            return []

    def is_valid_items_row(self, row_text: str) -> bool:
        """Valida se uma linha é um item válido"""
        try:
            text = row_text.strip().lower()

            # Rejeitar linhas inválidas
            invalid_patterns = [
                r'\\d{2}/\\d{2}/\\d{4}.*\\d{2}:\\d{2}:\\d{2}',
                r'\\.pdf$', r'\\.doc$', r'\\.rar$',
                r'^$', r'^\\s*$',
                r'inclusão.*contratação',
                r'inclusão.*documento',
                r'alteração.*',
                r'publicação.*'
            ]

            for pattern in invalid_patterns:
                if re.search(pattern, text):
                    return False

            # Aceitar linhas com estrutura de item
            valid_patterns = [
                r'\\d+.*[a-zA-Z]{3,}.*\\d+',
                r'[a-zA-Z]{3,}.*\\d+.*r\\$',
                r'[a-zA-Z]{3,}.*\\d+.*\\d+'
            ]

            for pattern in valid_patterns:
                if re.search(pattern, text):
                    words = text.split()
                    significant_words = [w for w in words if len(w) > 3 and w.isalpha()]
                    if len(significant_words) > 0:
                        return True

            if len(text) >= 10 and not re.match(r'^[\\d\\s/:-]+$', text):
                return True

            return False

        except Exception as e:
            return False

    async def extract_angular_row_data_corrected(self, row_element, index: int) -> Optional[Dict]:
        """Extrai dados de uma linha Angular - VERSÃO CORRIGIDA"""
        try:
            # Extrair células da linha
            cells = await row_element.locator('datatable-body-cell').all()

            if len(cells) < 3:
                return None

            # Extrair dados de cada célula
            cell_data = []
            for cell in cells:
                try:
                    # Procurar por spans Angular
                    spans = await cell.locator('span.ng-star-inserted').all()
                    cell_text = ""
                    
                    for span in spans:
                        span_text = await span.inner_text()
                        if span_text and span_text.strip():
                            cell_text = span_text.strip()
                            break

                    # Se não encontrou span, pegar texto da célula
                    if not cell_text:
                        cell_text = await cell.inner_text()
                        cell_text = cell_text.strip()

                    cell_data.append(cell_text)

                except Exception as e:
                    cell_data.append("")

            # Validar dados extraídos
            row_text = " ".join(cell_data)
            if not self.is_valid_items_row(row_text):
                return None

            # Processar dados CORRIGIDO
            if len(cell_data) >= 3:
                numero = cell_data[0] if cell_data[0].isdigit() else str(index + 1)
                descricao = cell_data[1] if len(cell_data[1]) > 5 else f"Item {index + 1}"

                # Extrair quantidade
                quantidade = None
                for data in cell_data[2:]:
                    if data.isdigit():
                        quantidade = int(data)
                        break

                # Extrair valores CORRIGIDO
                valor_unitario = None
                valor_total = None

                for data in cell_data:
                    if 'R$' in data:
                        try:
                            # Remover R$ e espaços, depois converter
                            valor_clean = data.replace('R$', '').strip()
                            # Tratar formato brasileiro: 1.234.567,89
                            if ',' in valor_clean:
                                # Separar parte inteira e decimal
                                parts = valor_clean.split(',')
                                if len(parts) == 2:
                                    # Remover pontos da parte inteira
                                    parte_inteira = parts[0].replace('.', '')
                                    parte_decimal = parts[1]
                                    valor_str = f"{parte_inteira}.{parte_decimal}"
                                else:
                                    valor_str = valor_clean.replace('.', '').replace(',', '.')
                            else:
                                valor_str = valor_clean.replace('.', '')
                            
                            valor_float = float(valor_str)
                            
                            if valor_unitario is None:
                                valor_unitario = valor_float
                            else:
                                valor_total = valor_float
                                
                        except Exception as e:
                            logger.warning(f"⚠️ Erro ao converter valor '{data}': {e}")
                            pass
                    elif data == "Sigiloso":
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
                    'extraction_method': 'angular_definitivo_extraction'
                }

            return None

        except Exception as e:
            logger.warning(f"⚠️ Erro ao extrair dados da linha Angular: {e}")
            return None

    async def extract_items_from_visible_text(self) -> List[Dict]:
        """Fallback: extrai itens do texto visível"""
        try:
            logger.info("📊 Fallback: extraindo itens do texto visível...")

            page_text = await self.page.inner_text('body')
            items = []
            lines = page_text.split('\\n')

            in_items_section = False
            for i, line in enumerate(lines):
                line = line.strip()

                if 'Número' in line and 'Descrição' in line and 'Quantidade' in line:
                    in_items_section = True
                    continue

                if in_items_section and ('Arquivos' in line or 'Histórico' in line or 'Voltar' in line):
                    break

                if in_items_section and self.is_valid_items_row(line):
                    item = self.parse_item_row_corrected(line, len(items))
                    if item:
                        items.append(item)

            return items[:10]

        except Exception as e:
            logger.warning(f"⚠️ Erro ao extrair itens do texto visível: {e}")
            return []

    def parse_item_row_corrected(self, row_text: str, index: int) -> Optional[Dict]:
        """Parse de linha de item - VERSÃO CORRIGIDA"""
        try:
            parts = re.split(r'\\t+|\\s{3,}', row_text.strip())
            parts = [p.strip() for p in parts if p.strip()]

            if len(parts) < 3:
                return None

            numero = parts[0] if parts[0].isdigit() else str(index + 1)

            # Encontrar descrição
            descricao = ""
            for part in parts[1:]:
                if len(part) > len(descricao) and not re.match(r'^[\\d\\s,.$R]+$', part):
                    descricao = part

            # Extrair valores CORRIGIDO
            valores = []
            for part in parts:
                if 'R$' in part:
                    try:
                        valor_clean = part.replace('R$', '').strip()
                        if ',' in valor_clean:
                            parts_valor = valor_clean.split(',')
                            if len(parts_valor) == 2:
                                parte_inteira = parts_valor[0].replace('.', '')
                                parte_decimal = parts_valor[1]
                                valor_str = f"{parte_inteira}.{parte_decimal}"
                            else:
                                valor_str = valor_clean.replace('.', '').replace(',', '.')
                        else:
                            valor_str = valor_clean.replace('.', '')
                        
                        valores.append(float(valor_str))
                    except:
                        pass

            # Extrair quantidade
            quantidade = None
            for part in parts:
                if re.match(r'^\\d+$', part.strip()) and part != numero:
                    quantidade = int(part)
                    break

            return {
                'numero': numero,
                'descricao': descricao[:500] if descricao else f"Item {index + 1}",
                'quantidade': quantidade,
                'valor_unitario': valores[0] if len(valores) > 0 else None,
                'valor_total': valores[1] if len(valores) > 1 else valores[0] if len(valores) > 0 else None,
                'extraction_method': 'text_fallback_definitivo'
            }

        except Exception as e:
            return None

    async def extract_files_complete(self, index: int, uf: str) -> Dict:
        """Extrai/baixa arquivos da aba 'Arquivos'"""
        try:
            logger.info("📁 Extraindo arquivos da aba 'Arquivos'...")

            # Tentar ativar aba Arquivos
            files_tab_found = await self.ensure_files_tab_active()

            if not files_tab_found:
                logger.warning("⚠️ Aba 'Arquivos' não encontrada")
                return {'files_tab_found': False, 'files': []}

            await self.page.wait_for_timeout(3000)

            if self.save_screenshots:
                await self.page.screenshot(path=f"debug_files_{uf}_{index}.png")

            # Extrair/baixar arquivos
            files = []
            if self.download_files:
                files = await self.download_files_from_tab(index, uf)
            else:
                files = await self.list_files_from_tab()

            logger.info(f"📁 Processados {len(files)} arquivos da aba 'Arquivos'")

            return {
                'files_tab_found': True,
                'files': files
            }

        except Exception as e:
            logger.error(f"❌ Erro ao extrair arquivos: {e}")
            return {'files_tab_found': False, 'files': []}

    async def ensure_files_tab_active(self) -> bool:
        """Garante que a aba 'Arquivos' está ativa"""
        try:
            logger.info("🎯 Ativando aba 'Arquivos'...")

            files_tab_selectors = [
                'li.tab-item:has-text("Arquivos")',
                'button:has-text("Arquivos")',
                '[class*="tab"]:has-text("Arquivos")',
                'li:has-text("Arquivos") button',
                '.tab-item:has-text("Arquivos") button'
            ]

            for selector in files_tab_selectors:
                try:
                    tab_button = self.page.locator(selector)

                    if await tab_button.count() > 0:
                        logger.info(f"✅ Aba 'Arquivos' encontrada! (Seletor: {selector})")

                        await tab_button.first.click()
                        await self.page.wait_for_timeout(3000)
                        logger.info("✅ Aba 'Arquivos' ativada")

                        return True

                except Exception as e:
                    continue

            logger.warning("⚠️ Aba 'Arquivos' não encontrada")
            return False

        except Exception as e:
            logger.error(f"❌ Erro ao ativar aba Arquivos: {e}")
            return False

    async def download_files_from_tab(self, index: int, uf: str) -> List[Dict]:
        """Baixa arquivos da aba Arquivos"""
        try:
            files = []

            # Procurar por links de download
            download_selectors = [
                'a[href*=".pdf"]',
                'a[href*=".doc"]',
                'a[href*=".rar"]',
                'a[href*=".zip"]',
                'a[download]',
                'button:has-text("Baixar")',
                'a:has-text("Download")'
            ]

            for selector in download_selectors:
                try:
                    links = await self.page.locator(selector).all()

                    for i, link in enumerate(links[:5]):  # Limitar a 5 arquivos
                        try:
                            # Obter informações do arquivo
                            link_text = await link.inner_text()
                            href = await link.get_attribute('href')

                            if href:
                                filename = f"{uf}_{index}_{i}_{link_text[:20]}.pdf"
                                filename = re.sub(r'[^\\w\\-_\\.]', '_', filename)

                                # Simular download (sem baixar realmente para performance)
                                file_info = {
                                    'filename': filename,
                                    'original_name': link_text,
                                    'url': href,
                                    'size': 0,  # Placeholder
                                    'download_status': 'simulated',
                                    'extraction_method': 'files_tab_definitivo'
                                }

                                files.append(file_info)
                                logger.info(f"   📄 Arquivo {i+1}: {link_text[:30]}...")

                        except Exception as e:
                            continue

                    if files:
                        break

                except Exception as e:
                    continue

            return files

        except Exception as e:
            logger.error(f"❌ Erro ao baixar arquivos: {e}")
            return []

    async def list_files_from_tab(self) -> List[Dict]:
        """Lista arquivos sem baixar"""
        try:
            files = []

            # Procurar por nomes de arquivos na aba
            file_text = await self.page.inner_text('body')
            lines = file_text.split('\\n')

            for i, line in enumerate(lines):
                line = line.strip()
                
                # Detectar nomes de arquivos
                if any(ext in line.lower() for ext in ['.pdf', '.doc', '.rar', '.zip']):
                    file_info = {
                        'filename': line[:50],
                        'original_name': line,
                        'url': '',
                        'size': 0,
                        'download_status': 'listed_only',
                        'extraction_method': 'files_tab_list_definitivo'
                    }
                    files.append(file_info)

            return files[:5]  # Limitar a 5 arquivos

        except Exception as e:
            logger.error(f"❌ Erro ao listar arquivos: {e}")
            return []

    async def scrape_estado(self, uf: str, limit: int = None) -> List[Dict]:
        """Faz scraping completo de um estado"""
        logger.info(f"\\n🏛️ Processando estado: {uf}")
        
        editais = []
        current_page = 1
        max_pages = 3
        
        # Navegar para o estado
        if not await self.navigate_and_filter(uf):
            logger.warning(f"❌ Falha ao acessar editais de {uf}")
            return []

        while len(editais) < (limit or 30) and current_page <= max_pages:
            logger.info(f"📄 Processando página {current_page} de {uf}")
            
            # Contar editais na página
            total_editais_page = await self.get_editais_count()
            
            if total_editais_page == 0:
                logger.info(f"📄 Página {current_page} de {uf} não tem editais")
                break

            # Processar editais da página
            editais_to_process = min(total_editais_page, (limit or 30) - len(editais))
            
            for i in range(editais_to_process):
                edital_info = await self.process_edital(i, uf)
                
                if edital_info:
                    editais.append(edital_info)
                    logger.info(f"✅ {uf} - Edital {len(editais)}: {edital_info.get('title', 'N/A')[:50]}...")
                    logger.info(f"   📊 PNCP ID: {edital_info.get('pncp_id', 'N/A')}")
                    logger.info(f"   📊 Órgão: {edital_info.get('organization_name', 'N/A')}")
                    logger.info(f"   📊 Município: {edital_info.get('municipality_name', 'N/A')}")
                    logger.info(f"   📊 Itens: {edital_info.get('items_count', 0)} | Arquivos: {edital_info.get('downloads_count', 0)}")
                
                # Verificar limite
                if limit and len(editais) >= limit:
                    break

            # Tentar próxima página
            if len(editais) < (limit or 30) and not await self.click_next_page():
                logger.info(f"📄 Não há mais páginas para {uf}")
                break
                
            current_page += 1

        logger.info(f"✅ Estado {uf} concluído: {len(editais)} editais coletados")
        return editais

    async def scrape_multiplos_estados(self, estados: List[str], limit_por_estado: int = 15) -> List[Dict]:
        """Faz scraping completo de múltiplos estados"""
        logger.info(f"🌎 Iniciando scraping definitivo de {len(estados)} estados")
        
        all_editais = []
        
        for i, uf in enumerate(estados, 1):
            logger.info(f"\\n📍 Estado {i}/{len(estados)}: {uf}")
            
            try:
                editais_estado = await self.scrape_estado(uf, limit_por_estado)
                all_editais.extend(editais_estado)
                
                # Estatísticas do estado
                total_items = sum(e.get('items_count', 0) for e in editais_estado)
                total_files = sum(e.get('downloads_count', 0) for e in editais_estado)
                
                logger.info(f"✅ {uf}: {len(editais_estado)} editais | {total_items} itens | {total_files} arquivos")
                logger.info(f"📊 Total geral: {len(all_editais)} editais")
                
                # Pausa entre estados
                if i < len(estados):
                    await asyncio.sleep(2)
                    
            except Exception as e:
                logger.error(f"❌ Erro ao processar {uf}: {e}")
                continue

        return all_editais

    def conectar_banco(self):
        """Conecta ao banco PostgreSQL"""
        try:
            conn = psycopg2.connect(
                host=os.getenv('DB_HOST'),
                port=os.getenv('DB_PORT', 5432),
                database=os.getenv('DB_NAME'),
                user=os.getenv('DB_USER'),
                password=os.getenv('DB_PASSWORD'),
                client_encoding='utf8'
            )
            return conn
        except Exception as e:
            logger.error(f"❌ Erro ao conectar banco: {e}")
            return None

    def inserir_no_banco(self, editais: List[Dict]) -> bool:
        """Insere editais completos no banco"""
        logger.info(f"💾 Inserindo {len(editais)} editais definitivos no banco...")
        
        conn = self.conectar_banco()
        if not conn:
            return False
        
        try:
            cursor = conn.cursor()
            
            # Obter IDs existentes
            cursor.execute("SELECT pncp_id FROM tenders WHERE pncp_id IS NOT NULL")
            existing_ids = set(row[0] for row in cursor.fetchall())
            
            inseridos = 0
            atualizados = 0
            
            for edital in editais:
                pncp_id = edital.get('pncp_id', '')
                
                # Preparar dados completos
                dados = {
                    'pncp_id': pncp_id,
                    'title': edital.get('title', ''),
                    'description': edital.get('description', ''),
                    'organization_name': edital.get('organization_name', ''),
                    'municipality_name': edital.get('municipality_name', ''),
                    'state_code': edital.get('state_code', ''),
                    'status': edital.get('status', ''),
                    'modality': edital.get('modality', ''),
                    'estimated_value': edital.get('estimated_value'),
                    'valor_total_estimado': edital.get('valor_total_estimado'),
                    'source_url': edital.get('source_url', ''),
                    'detail_url': edital.get('detail_url', ''),
                    'data_source': edital.get('data_source', ''),
                    'objeto': edital.get('objeto', ''),
                    'detailed_description': edital.get('detailed_description', ''),
                    'items_json': json.dumps(edital.get('items', []), ensure_ascii=False),
                    'downloaded_files_json': json.dumps(edital.get('downloaded_files', []), ensure_ascii=False),
                    'items_count': len(edital.get('items', [])),
                    'downloads_count': len(edital.get('downloaded_files', [])),
                    'created_at': datetime.now()
                }
                
                # Tratar data
                try:
                    dados['publication_date'] = datetime.strptime(edital.get('publication_date', ''), '%Y-%m-%d').date()
                except:
                    dados['publication_date'] = datetime.now().date()
                
                try:
                    if pncp_id and pncp_id in existing_ids:
                        # Atualizar com dados completos
                        cursor.execute("""
                            UPDATE tenders SET
                                title = %(title)s,
                                description = %(description)s,
                                organization_name = %(organization_name)s,
                                municipality_name = %(municipality_name)s,
                                state_code = %(state_code)s,
                                publication_date = %(publication_date)s,
                                status = %(status)s,
                                modality = %(modality)s,
                                estimated_value = %(estimated_value)s,
                                valor_total_estimado = %(valor_total_estimado)s,
                                source_url = %(source_url)s,
                                detail_url = %(detail_url)s,
                                objeto = %(objeto)s,
                                detailed_description = %(detailed_description)s,
                                items_json = %(items_json)s,
                                downloaded_files_json = %(downloaded_files_json)s,
                                items_count = %(items_count)s,
                                downloads_count = %(downloads_count)s
                            WHERE pncp_id = %(pncp_id)s
                        """, dados)
                        atualizados += 1
                    else:
                        # Inserir novo com dados completos
                        cursor.execute("""
                            INSERT INTO tenders (
                                pncp_id, title, description, organization_name,
                                municipality_name, state_code, publication_date,
                                status, modality, estimated_value, valor_total_estimado,
                                source_url, detail_url, data_source, objeto,
                                detailed_description, items_json, downloaded_files_json, 
                                items_count, downloads_count, created_at
                            ) VALUES (
                                %(pncp_id)s, %(title)s, %(description)s, %(organization_name)s,
                                %(municipality_name)s, %(state_code)s, %(publication_date)s,
                                %(status)s, %(modality)s, %(estimated_value)s, %(valor_total_estimado)s,
                                %(source_url)s, %(detail_url)s, %(data_source)s, %(objeto)s,
                                %(detailed_description)s, %(items_json)s, %(downloaded_files_json)s,
                                %(items_count)s, %(downloads_count)s, %(created_at)s
                            )
                        """, dados)
                        inseridos += 1
                        
                except Exception as e:
                    logger.error(f"❌ Erro ao processar {pncp_id}: {e}")
                    continue
            
            conn.commit()
            conn.close()
            
            logger.info(f"✅ Banco atualizado:")
            logger.info(f"   - Novos: {inseridos}")
            logger.info(f"   - Atualizados: {atualizados}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Erro ao inserir no banco: {e}")
            return False

    def salvar_json(self, editais: List[Dict]) -> str:
        """Salva editais completos em arquivo JSON"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"editais_definitivo_{timestamp}.json"
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(editais, f, ensure_ascii=False, indent=2, default=str)
            
            logger.info(f"💾 Arquivo JSON definitivo salvo: {filename}")
            return filename
            
        except Exception as e:
            logger.error(f"❌ Erro ao salvar JSON: {e}")
            return ""


async def main():
    """Função principal"""
    logger.info("🚀 PNCP Scraper Definitivo - Extração Correta Completa")
    logger.info("=" * 70)
    
    # Configurações
    HEADLESS = True
    SAVE_SCREENSHOTS = False
    DOWNLOAD_FILES = False    # Simular downloads para performance
    LIMIT_POR_ESTADO = 3      # Limite menor para teste
    
    # Opções de execução
    print("\\n📋 Opções de execução:")
    print("1. Estados prioritários (SP, RJ, MG, RS, PR, SC, BA, GO, DF)")
    print("2. Todos os estados")
    print("3. Estado específico")
    print("4. Apenas SP (teste definitivo)")
    
    try:
        opcao = input("\\nEscolha uma opção (1-4): ").strip()
        
        async with PNCPScraperDefinitivo(
            headless=HEADLESS, 
            save_screenshots=SAVE_SCREENSHOTS,
            download_files=DOWNLOAD_FILES
        ) as scraper:
            editais = []
            
            if opcao == "1":
                logger.info("🎯 Executando estados prioritários...")
                editais = await scraper.scrape_multiplos_estados(scraper.estados_prioritarios, LIMIT_POR_ESTADO)
                
            elif opcao == "2":
                logger.info("🌎 Executando todos os estados...")
                editais = await scraper.scrape_multiplos_estados(scraper.estados, LIMIT_POR_ESTADO)
                
            elif opcao == "3":
                uf = input("Digite o código do estado (ex: SP): ").strip().upper()
                if uf in scraper.estados:
                    logger.info(f"🏛️ Executando apenas {uf}...")
                    editais = await scraper.scrape_estado(uf, LIMIT_POR_ESTADO * 2)
                else:
                    logger.error(f"❌ Estado inválido: {uf}")
                    return
                    
            elif opcao == "4":
                logger.info("🧪 Teste definitivo com SP...")
                editais = await scraper.scrape_estado("SP", 3)  # 3 editais para teste
                
            else:
                logger.error("❌ Opção inválida")
                return
            
            # Processar resultados
            if editais:
                logger.info(f"\\n🎉 Scraping definitivo concluído!")
                logger.info(f"📊 Total de editais coletados: {len(editais)}")
                
                # Estatísticas detalhadas
                total_items = sum(edital.get('items_count', 0) for edital in editais)
                total_files = sum(edital.get('downloads_count', 0) for edital in editais)
                editais_com_itens = sum(1 for e in editais if e.get('items_count', 0) > 0)
                editais_com_arquivos = sum(1 for e in editais if e.get('downloads_count', 0) > 0)
                estados_processados = len(set(edital.get('state_code') for edital in editais))
                
                # Verificar se informações básicas foram extraídas CORRETAMENTE
                editais_com_pncp_id = sum(1 for e in editais if e.get('pncp_id', ''))
                editais_com_orgao = sum(1 for e in editais if e.get('organization_name', ''))
                editais_com_municipio = sum(1 for e in editais if e.get('municipality_name', ''))
                editais_com_modalidade = sum(1 for e in editais if e.get('modality', ''))
                editais_com_objeto = sum(1 for e in editais if e.get('objeto', ''))
                editais_com_valor = sum(1 for e in editais if e.get('valor_total_estimado'))
                
                logger.info(f"\\n📊 Estatísticas definitivas:")
                logger.info(f"   - Editais coletados: {len(editais)}")
                logger.info(f"   - Estados processados: {estados_processados}")
                logger.info(f"   - Editais com PNCP ID: {editais_com_pncp_id}")
                logger.info(f"   - Editais com Órgão: {editais_com_orgao}")
                logger.info(f"   - Editais com Município: {editais_com_municipio}")
                logger.info(f"   - Editais com Modalidade: {editais_com_modalidade}")
                logger.info(f"   - Editais com Objeto: {editais_com_objeto}")
                logger.info(f"   - Editais com Valor Total: {editais_com_valor}")
                logger.info(f"   - Editais com itens: {editais_com_itens}")
                logger.info(f"   - Total de itens extraídos: {total_items}")
                logger.info(f"   - Editais com arquivos: {editais_com_arquivos}")
                logger.info(f"   - Total de arquivos processados: {total_files}")
                
                # Salvar JSON
                json_file = scraper.salvar_json(editais)
                
                # Inserir no banco
                if editais:
                    scraper.inserir_no_banco(editais)
                
                # Mostrar exemplos de dados extraídos CORRETAMENTE
                logger.info(f"\\n📋 Exemplos de dados extraídos CORRETAMENTE:")
                for i, edital in enumerate(editais[:2]):
                    logger.info(f"   Edital {i+1}: {edital.get('title', 'N/A')[:40]}...")
                    logger.info(f"      PNCP ID: {edital.get('pncp_id', 'N/A')}")
                    logger.info(f"      Órgão: {edital.get('organization_name', 'N/A')}")
                    logger.info(f"      Município: {edital.get('municipality_name', 'N/A')}")
                    logger.info(f"      Modalidade: {edital.get('modality', 'N/A')}")
                    logger.info(f"      Objeto: {edital.get('objeto', 'N/A')[:50]}...")
                    logger.info(f"      Valor Total: R$ {edital.get('valor_total_estimado', 0):,.2f}" if edital.get('valor_total_estimado') else "      Valor Total: N/A")
                    logger.info(f"      Itens: {edital.get('items_count', 0)}")
                    logger.info(f"      Arquivos: {edital.get('downloads_count', 0)}")
                
                logger.info(f"\\n💾 Arquivo JSON: {json_file}")
                
            else:
                logger.warning("❌ Nenhum edital foi coletado")
                
    except KeyboardInterrupt:
        logger.info("\\n⏹️ Execução interrompida pelo usuário")
    except Exception as e:
        logger.error(f"❌ Erro: {e}")


if __name__ == "__main__":
    asyncio.run(main())
