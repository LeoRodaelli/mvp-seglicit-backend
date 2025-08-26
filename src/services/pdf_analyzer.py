import os
import re
import json
import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime
from decimal import Decimal

try:
    import fitz  # PyMuPDF
    import pdfplumber
except ImportError:
    print("ERRO: Dependências de PDF não instaladas. Execute: pip install PyMuPDF pdfplumber")
    exit(1)

logger = logging.getLogger(__name__)

class PDFAnalyzer:
    """Serviço para análise semântica de PDFs de editais"""
    
    def __init__(self):
        self.patterns = self._init_patterns()
    
    def _init_patterns(self) -> Dict[str, re.Pattern]:
        """Inicializa padrões regex para extração de dados"""
        return {
            # Valores monetários
            'valor_estimado': re.compile(r'valor\s+(?:estimado|total|global|máximo)[\s:]*R?\$?\s*([\d.,]+)', re.IGNORECASE),
            'valor_unitario': re.compile(r'valor\s+unitário[\s:]*R?\$?\s*([\d.,]+)', re.IGNORECASE),
            'valor_total': re.compile(r'valor\s+total[\s:]*R?\$?\s*([\d.,]+)', re.IGNORECASE),
            
            # Datas
            'data_abertura': re.compile(r'(?:data|abertura).*?(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4})', re.IGNORECASE),
            'data_entrega': re.compile(r'(?:prazo|entrega|execução).*?(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4})', re.IGNORECASE),
            'data_publicacao': re.compile(r'(?:publicado|publicação).*?(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4})', re.IGNORECASE),
            
            # Informações do edital
            'numero_edital': re.compile(r'(?:edital|pregão|concorrência)\s*n[ºo°]?\s*(\d+[\/\-]\d+)', re.IGNORECASE),
            'numero_processo': re.compile(r'processo\s*n[ºo°]?\s*([\d\/\-\.]+)', re.IGNORECASE),
            'modalidade': re.compile(r'(pregão|concorrência|tomada\s+de\s+preços|convite|concurso)', re.IGNORECASE),
            
            # Contatos
            'email': re.compile(r'[\w\.-]+@[\w\.-]+\.\w+'),
            'telefone': re.compile(r'\(?\d{2}\)?\s*\d{4,5}[\-\s]?\d{4}'),
            'endereco': re.compile(r'(?:rua|av|avenida|praça)[\s\w\d,\-\.]+', re.IGNORECASE),
            
            # Objeto
            'objeto': re.compile(r'objeto[\s:]*(.{10,200})', re.IGNORECASE | re.DOTALL),
            'descricao': re.compile(r'descrição[\s:]*(.{10,300})', re.IGNORECASE | re.DOTALL),
            
            # Prazos
            'prazo_execucao': re.compile(r'prazo.*?(\d+)\s*(?:dias|meses|anos)', re.IGNORECASE),
            'prazo_entrega': re.compile(r'entrega.*?(\d+)\s*(?:dias|meses|anos)', re.IGNORECASE),
            
            # Garantias
            'garantia': re.compile(r'garantia.*?([\d,]+)%', re.IGNORECASE),
            
            # Critérios
            'criterio_julgamento': re.compile(r'critério.*?julgamento[\s:]*(.{10,100})', re.IGNORECASE),
        }
    
    def analyze_pdf(self, pdf_path: str) -> Dict:
        """
        Analisa um PDF e extrai dados semânticos
        
        Args:
            pdf_path: Caminho para o arquivo PDF
            
        Returns:
            Dict com dados extraídos
        """
        try:
            logger.info(f"Analisando PDF: {pdf_path}")
            
            if not os.path.exists(pdf_path):
                raise FileNotFoundError(f"Arquivo não encontrado: {pdf_path}")
            
            # Extrair texto usando múltiplas bibliotecas
            text_pymupdf = self._extract_text_pymupdf(pdf_path)
            text_pdfplumber = self._extract_text_pdfplumber(pdf_path)
            
            # Combinar textos para análise mais robusta
            combined_text = f"{text_pymupdf}\n\n{text_pdfplumber}"
            
            # Extrair dados semânticos
            semantic_data = self._extract_semantic_data(combined_text)
            
            # Extrair tabelas
            tables = self._extract_tables_pdfplumber(pdf_path)
            
            # Metadados do arquivo
            file_info = self._get_file_info(pdf_path)
            
            result = {
                'file_info': file_info,
                'text_length': len(combined_text),
                'text_preview': combined_text[:500] + "..." if len(combined_text) > 500 else combined_text,
                'semantic_data': semantic_data,
                'tables': tables,
                'extraction_method': 'pymupdf_pdfplumber_combined',
                'analyzed_at': datetime.now().isoformat()
            }
            
            logger.info(f"Análise concluída. Dados extraídos: {len(semantic_data)} campos")
            return result
            
        except Exception as e:
            logger.error(f"Erro ao analisar PDF {pdf_path}: {str(e)}")
            return {
                'error': str(e),
                'file_info': self._get_file_info(pdf_path) if os.path.exists(pdf_path) else {},
                'analyzed_at': datetime.now().isoformat()
            }
    
    def _extract_text_pymupdf(self, pdf_path: str) -> str:
        """Extrai texto usando PyMuPDF"""
        try:
            doc = fitz.open(pdf_path)
            text = ""
            for page in doc:
                text += page.get_text()
            doc.close()
            return text
        except Exception as e:
            logger.warning(f"Erro ao extrair texto com PyMuPDF: {str(e)}")
            return ""
    
    def _extract_text_pdfplumber(self, pdf_path: str) -> str:
        """Extrai texto usando pdfplumber"""
        try:
            text = ""
            with pdfplumber.open(pdf_path) as pdf:
                for page in pdf.pages:
                    page_text = page.extract_text()
                    if page_text:
                        text += page_text + "\n"
            return text
        except Exception as e:
            logger.warning(f"Erro ao extrair texto com pdfplumber: {str(e)}")
            return ""
    
    def _extract_tables_pdfplumber(self, pdf_path: str) -> List[Dict]:
        """Extrai tabelas usando pdfplumber"""
        try:
            tables = []
            with pdfplumber.open(pdf_path) as pdf:
                for page_num, page in enumerate(pdf.pages, 1):
                    page_tables = page.extract_tables()
                    for table_num, table in enumerate(page_tables, 1):
                        if table and len(table) > 1:  # Pelo menos cabeçalho + 1 linha
                            tables.append({
                                'page': page_num,
                                'table_number': table_num,
                                'headers': table[0] if table else [],
                                'rows': table[1:] if len(table) > 1 else [],
                                'total_rows': len(table) - 1 if table else 0
                            })
            return tables
        except Exception as e:
            logger.warning(f"Erro ao extrair tabelas: {str(e)}")
            return []
    
    def _extract_semantic_data(self, text: str) -> Dict:
        """Extrai dados semânticos do texto usando regex"""
        data = {}
        
        for field_name, pattern in self.patterns.items():
            matches = pattern.findall(text)
            if matches:
                if field_name.startswith('valor_'):
                    # Processar valores monetários
                    data[field_name] = self._process_monetary_values(matches)
                elif field_name.startswith('data_'):
                    # Processar datas
                    data[field_name] = self._process_dates(matches)
                elif field_name in ['objeto', 'descricao', 'criterio_julgamento']:
                    # Processar textos longos
                    data[field_name] = self._clean_text(matches[0]) if matches else None
                elif field_name.startswith('prazo_'):
                    # Processar prazos
                    data[field_name] = self._process_deadlines(matches)
                else:
                    # Outros campos
                    data[field_name] = matches[0] if len(matches) == 1 else matches
        
        return data
    
    def _process_monetary_values(self, matches: List[str]) -> Optional[float]:
        """Processa valores monetários"""
        try:
            if not matches:
                return None
            
            # Pegar o primeiro valor encontrado
            value_str = matches[0]
            
            # Limpar e converter
            clean_value = value_str.replace('.', '').replace(',', '.')
            clean_value = re.sub(r'[^\d.]', '', clean_value)
            
            if clean_value:
                return float(clean_value)
            return None
        except:
            return None
    
    def _process_dates(self, matches: List[str]) -> Optional[str]:
        """Processa datas"""
        try:
            if not matches:
                return None
            
            date_str = matches[0]
            # Normalizar formato de data
            date_str = re.sub(r'(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{2,4})', r'\1/\2/\3', date_str)
            return date_str
        except:
            return None
    
    def _process_deadlines(self, matches: List[str]) -> Optional[Dict]:
        """Processa prazos"""
        try:
            if not matches:
                return None
            
            value = int(matches[0])
            return {'value': value, 'unit': 'dias'}  # Assumir dias por padrão
        except:
            return None
    
    def _clean_text(self, text: str) -> str:
        """Limpa e normaliza texto"""
        if not text:
            return ""
        
        # Remover quebras de linha excessivas
        text = re.sub(r'\n+', ' ', text)
        # Remover espaços múltiplos
        text = re.sub(r'\s+', ' ', text)
        # Remover caracteres especiais no início/fim
        text = text.strip(' .:;,-')
        
        return text[:500]  # Limitar tamanho
    
    def _get_file_info(self, pdf_path: str) -> Dict:
        """Obtém informações do arquivo"""
        try:
            stat = os.stat(pdf_path)
            return {
                'filename': os.path.basename(pdf_path),
                'size_bytes': stat.st_size,
                'size_mb': round(stat.st_size / (1024 * 1024), 2),
                'modified_at': datetime.fromtimestamp(stat.st_mtime).isoformat(),
                'path': pdf_path
            }
        except:
            return {'filename': os.path.basename(pdf_path), 'path': pdf_path}
    
    def analyze_multiple_pdfs(self, pdf_paths: List[str]) -> Dict[str, Dict]:
        """Analisa múltiplos PDFs"""
        results = {}
        
        for pdf_path in pdf_paths:
            filename = os.path.basename(pdf_path)
            results[filename] = self.analyze_pdf(pdf_path)
        
        return results
    
    def extract_items_from_tables(self, tables: List[Dict]) -> List[Dict]:
        """Extrai itens de licitação das tabelas"""
        items = []
        
        for table in tables:
            headers = table.get('headers', [])
            rows = table.get('rows', [])
            
            # Procurar por colunas relevantes
            item_columns = self._identify_item_columns(headers)
            
            if item_columns:
                for row in rows:
                    if len(row) >= len(headers):
                        item = self._extract_item_from_row(row, headers, item_columns)
                        if item:
                            items.append(item)
        
        return items
    
    def _identify_item_columns(self, headers: List[str]) -> Dict[str, int]:
        """Identifica colunas relevantes para itens"""
        columns = {}
        
        for i, header in enumerate(headers):
            if not header:
                continue
                
            header_lower = header.lower()
            
            if any(word in header_lower for word in ['item', 'número', 'nº']):
                columns['numero'] = i
            elif any(word in header_lower for word in ['descrição', 'objeto', 'especificação']):
                columns['descricao'] = i
            elif any(word in header_lower for word in ['quantidade', 'qtd', 'qtde']):
                columns['quantidade'] = i
            elif any(word in header_lower for word in ['unitário', 'unit']):
                columns['valor_unitario'] = i
            elif any(word in header_lower for word in ['total', 'valor total']):
                columns['valor_total'] = i
            elif any(word in header_lower for word in ['unidade', 'un', 'medida']):
                columns['unidade'] = i
        
        return columns
    
    def _extract_item_from_row(self, row: List[str], headers: List[str], columns: Dict[str, int]) -> Optional[Dict]:
        """Extrai item de uma linha da tabela"""
        try:
            item = {}
            
            for field, col_index in columns.items():
                if col_index < len(row) and row[col_index]:
                    value = row[col_index].strip()
                    
                    if field in ['quantidade']:
                        try:
                            item[field] = int(float(value.replace(',', '.')))
                        except:
                            item[field] = None
                    elif field in ['valor_unitario', 'valor_total']:
                        try:
                            clean_value = re.sub(r'[^\d,.]', '', value)
                            clean_value = clean_value.replace(',', '.')
                            item[field] = float(clean_value) if clean_value else None
                        except:
                            item[field] = None
                    else:
                        item[field] = value
            
            # Só retornar se tiver pelo menos descrição
            if item.get('descricao'):
                return item
            
            return None
            
        except Exception as e:
            logger.warning(f"Erro ao extrair item da linha: {str(e)}")
            return None

# Função de conveniência para uso direto
def analyze_pdf_file(pdf_path: str) -> Dict:
    """Função de conveniência para analisar um PDF"""
    analyzer = PDFAnalyzer()
    return analyzer.analyze_pdf(pdf_path)

