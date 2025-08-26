import os
import sys
import asyncio
import logging
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Dict, List, Optional
import json

# Adicionar o diretório pai ao path para importações
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(os.path.dirname(current_dir))
sys.path.insert(0, parent_dir)

from src.models.edital import Edital, EditalItem, EditalFile
from src.models.user import db

logger = logging.getLogger(__name__)


class ScraperIntegrationService:
    """Serviço de integração do scraper com o sistema"""

    def __init__(self):
        self.app = None
        self.scraper = None

    def init_app(self, app):
        """Inicializa o serviço com a aplicação Flask"""
        self.app = app

        # Tentar encontrar o scraper em vários locais possíveis
        possible_paths = [
            # Caminho relativo ao diretório src
            os.path.join(os.path.dirname(os.path.dirname(__file__)), 'pncp_scraper_items_only.py'),
            # Caminho relativo ao diretório raiz do projeto
            os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'pncp_scraper_items_only.py'),
            # Caminho no diretório atual
            os.path.join(os.getcwd(), 'pncp_scraper_items_only.py'),
            # Caminho no diretório do backend
            os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'mvp-licitacoes-backend',
                         'pncp_scraper_items_only.py')
        ]

        scraper_path = None
        for path in possible_paths:
            if os.path.exists(path):
                scraper_path = path
                break

        if scraper_path:
            try:
                # Carregar o módulo do scraper dinamicamente
                import importlib.util
                spec = importlib.util.spec_from_file_location("pncp_scraper", scraper_path)
                scraper_module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(scraper_module)

                # Tentar diferentes nomes de classe
                scraper_class = None
                for class_name in ['PNCPScraperItemsOnly', 'PNCPScraper']:
                    if hasattr(scraper_module, class_name):
                        scraper_class = getattr(scraper_module, class_name)
                        break

                if scraper_class:
                    self.scraper = scraper_class(headless=True)
                    logger.info(f"Scraper inicializado com sucesso: {scraper_path}")
                else:
                    logger.warning(f"Classe do scraper não encontrada em: {scraper_path}")
                    self.scraper = None

            except Exception as e:
                logger.error(f"Erro ao inicializar scraper: {str(e)}")
                self.scraper = None
        else:
            logger.warning("Arquivo do scraper não encontrado em nenhum local")
            logger.info("Locais verificados:")
            for path in possible_paths:
                logger.info(f"  - {path}")
            self.scraper = None

    def safe_decimal_conversion(self, value):
        """Converte valor para Decimal de forma segura"""
        if value is None:
            return None

        # Se já é Decimal, retorna
        if isinstance(value, Decimal):
            return value

        # Converter para string primeiro
        str_value = str(value).strip()

        # Verificar se é "Sigiloso" ou similar
        if str_value.lower() in ['sigiloso', 'sigilosa', 'n/a', 'não informado', '']:
            return None

        try:
            # Remover caracteres não numéricos exceto vírgula e ponto
            import re
            clean_value = re.sub(r'[^\d,.]', '', str_value)

            # Substituir vírgula por ponto se necessário
            if ',' in clean_value and '.' not in clean_value:
                clean_value = clean_value.replace(',', '.')
            elif ',' in clean_value and '.' in clean_value:
                # Formato brasileiro: 1.234,56
                parts = clean_value.split(',')
                if len(parts) == 2:
                    integer_part = parts[0].replace('.', '')
                    decimal_part = parts[1]
                    clean_value = f"{integer_part}.{decimal_part}"

            if clean_value:
                return Decimal(clean_value)
            else:
                return None

        except (InvalidOperation, ValueError) as e:
            logger.warning(f"Erro ao converter valor '{value}' para Decimal: {str(e)}")
            return None

    async def run_scraping_async(self, estados: List[str] = None, limit: int = 5):
        """Executa o scraping de forma assíncrona"""
        if not self.scraper:
            return {
                'success': False,
                'error': 'Scraper não inicializado. Verifique se o arquivo pncp_scraper_items_only.py está no local correto.',
                'timestamp': datetime.now().isoformat()
            }

        if not estados:
            estados = ['SP']

        try:
            logger.info(f"Iniciando scraping assíncrono - Estados: {estados}, Limite: {limit}")

            # Executar scraping para o primeiro estado
            uf = estados[0]

            async with self.scraper:
                # Navegar e filtrar
                success = await self.scraper.navigate_and_filter(uf)
                if not success:
                    return {
                        'success': False,
                        'error': f'Falha ao navegar e filtrar por UF: {uf}',
                        'timestamp': datetime.now().isoformat()
                    }

                # Extrair editais
                editais_data = await self.scraper.extract_editais_from_page(limit)

                # Salvar no banco
                editais_salvos = 0
                erros = 0

                with self.app.app_context():
                    for edital_data in editais_data:
                        try:
                            # Verificar se edital já existe
                            existing = Edital.query.filter_by(
                                title=edital_data.get('titulo', ''),
                                organization_name=edital_data.get('organizacao', '')
                            ).first()

                            if not existing:
                                edital = Edital(
                                    title=edital_data.get('titulo', ''),
                                    description=edital_data.get('descricao', ''),
                                    object_description=edital_data.get('objeto', ''),
                                    organization_name=edital_data.get('organizacao', ''),
                                    municipality_name=edital_data.get('municipio', ''),
                                    state_code=edital_data.get('uf', ''),
                                    modality=edital_data.get('modalidade', ''),
                                    status=edital_data.get('status', 'Ativo'),
                                    estimated_value=self.safe_decimal_conversion(edital_data.get('valor_estimado')),
                                    source_url=edital_data.get('url_pncp', ''),
                                    data_source='PNCP',
                                    has_items_tab=len(edital_data.get('items', [])) > 0,
                                    has_files_tab=len(edital_data.get('files', [])) > 0
                                )

                                # Adicionar itens
                                for item_data in edital_data.get('items', []):
                                    item = EditalItem(
                                        numero=item_data.get('numero_item', ''),
                                        descricao=item_data.get('descricao', ''),
                                        quantidade=item_data.get('quantidade'),
                                        valor_unitario=self.safe_decimal_conversion(item_data.get('valor_unitario')),
                                        valor_total=self.safe_decimal_conversion(item_data.get('valor_total')),
                                        raw_data=json.dumps(item_data),
                                        extraction_method='PNCP_SCRAPER'
                                    )
                                    edital.items.append(item)

                                # Adicionar arquivos
                                for file_data in edital_data.get('files', []):
                                    file_obj = EditalFile(
                                        filename=file_data.get('filename', ''),
                                        original_url=file_data.get('original_url', ''),
                                        local_path=file_data.get('local_path', ''),
                                        file_size=file_data.get('file_size'),
                                        file_type=file_data.get('file_type', '')
                                    )
                                    edital.files.append(file_obj)

                                db.session.add(edital)
                                editais_salvos += 1

                        except Exception as e:
                            logger.error(f"Erro ao salvar edital: {str(e)}")
                            erros += 1

                    db.session.commit()

                logger.info(f"Scraping concluído - Salvos: {editais_salvos}, Erros: {erros}")

                return {
                    'success': True,
                    'editais_salvos': editais_salvos,
                    'erros': erros,
                    'total_processados': len(editais_data),
                    'timestamp': datetime.now().isoformat()
                }

        except Exception as e:
            logger.error(f"Erro no scraping: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }

    def run_scraping(self, estados: List[str] = None, limit: int = 5):
        """Executa o scraping (wrapper síncrono)"""
        try:
            # Executar de forma assíncrona
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            result = loop.run_until_complete(self.run_scraping_async(estados, limit))
            loop.close()
            return result
        except Exception as e:
            logger.error(f"Erro ao executar scraping: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }

    def load_json_to_database(self, json_path: str):
        """Carrega dados de um arquivo JSON para o banco"""
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            with self.app.app_context():
                editais_salvos = 0

                for edital_data in data:
                    # Verificar se edital já existe
                    existing = Edital.query.filter_by(
                        title=edital_data.get('title', ''),
                        organization_name=edital_data.get('organization_name', '')
                    ).first()

                    if not existing:
                        edital = Edital(
                            title=edital_data.get('title', ''),
                            description=edital_data.get('description', ''),
                            object_description=edital_data.get('object_description', ''),
                            organization_name=edital_data.get('organization_name', ''),
                            municipality_name=edital_data.get('municipality_name', ''),
                            state_code=edital_data.get('state_code', ''),
                            modality=edital_data.get('modality', ''),
                            status=edital_data.get('status', 'Ativo'),
                            estimated_value=self.safe_decimal_conversion(edital_data.get('estimated_value')),
                            source_url=edital_data.get('source_url', ''),
                            data_source='JSON_IMPORT',
                            has_items_tab=len(edital_data.get('items', [])) > 0,
                            has_files_tab=len(edital_data.get('files', [])) > 0
                        )

                        # Adicionar itens
                        for item_data in edital_data.get('items', []):
                            item = EditalItem(
                                numero=item_data.get('numero', ''),
                                descricao=item_data.get('descricao', ''),
                                quantidade=item_data.get('quantidade'),
                                valor_unitario=self.safe_decimal_conversion(item_data.get('valor_unitario')),
                                valor_total=self.safe_decimal_conversion(item_data.get('valor_total')),
                                raw_data=json.dumps(item_data),
                                extraction_method='JSON_IMPORT'
                            )
                            edital.items.append(item)

                        # Adicionar arquivos
                        for file_data in edital_data.get('files', []):
                            file_obj = EditalFile(
                                filename=file_data.get('filename', ''),
                                original_url=file_data.get('original_url', ''),
                                local_path=file_data.get('local_path', ''),
                                file_size=file_data.get('file_size'),
                                file_type=file_data.get('file_type', '')
                            )
                            edital.files.append(file_obj)

                        db.session.add(edital)
                        editais_salvos += 1

                db.session.commit()

                return {
                    'success': True,
                    'editais_salvos': editais_salvos,
                    'timestamp': datetime.now().isoformat()
                }

        except Exception as e:
            logger.error(f"Erro ao carregar JSON: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
