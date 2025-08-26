import os
import json
import logging
from typing import Dict, List, Optional
from datetime import datetime

from src.models.user import db
from src.models.edital import Edital, EditalFile
from src.services.pdf_analyzer import PDFAnalyzer

logger = logging.getLogger(__name__)

class PDFIntegrationService:
    """Serviço para integrar análise de PDF com o sistema de editais"""
    
    def __init__(self):
        self.analyzer = PDFAnalyzer()
    
    def analyze_edital_files(self, edital_id: int) -> Dict:
        """
        Analisa todos os arquivos PDF de um edital
        
        Args:
            edital_id: ID do edital
            
        Returns:
            Dict com resultados da análise
        """
        try:
            logger.info(f"Analisando arquivos do edital {edital_id}")
            
            # Buscar edital
            edital = Edital.query.get(edital_id)
            if not edital:
                raise ValueError(f"Edital {edital_id} não encontrado")
            
            # Buscar arquivos PDF do edital
            pdf_files = EditalFile.query.filter_by(
                edital_id=edital_id,
                file_type='PDF'
            ).all()
            
            if not pdf_files:
                logger.warning(f"Nenhum arquivo PDF encontrado para edital {edital_id}")
                return {
                    'edital_id': edital_id,
                    'files_analyzed': 0,
                    'results': {},
                    'message': 'Nenhum arquivo PDF encontrado'
                }
            
            results = {}
            analyzed_count = 0
            
            for pdf_file in pdf_files:
                if pdf_file.local_path and os.path.exists(pdf_file.local_path):
                    logger.info(f"Analisando arquivo: {pdf_file.filename}")
                    
                    # Analisar PDF
                    analysis_result = self.analyzer.analyze_pdf(pdf_file.local_path)
                    
                    # Salvar resultado no banco
                    self._save_analysis_to_database(pdf_file, analysis_result)
                    
                    results[pdf_file.filename] = analysis_result
                    analyzed_count += 1
                else:
                    logger.warning(f"Arquivo não encontrado: {pdf_file.local_path}")
            
            # Atualizar dados do edital com informações extraídas
            self._update_edital_with_analysis(edital, results)
            
            logger.info(f"Análise concluída. {analyzed_count} arquivos analisados")
            
            return {
                'edital_id': edital_id,
                'files_analyzed': analyzed_count,
                'results': results,
                'message': f'{analyzed_count} arquivos analisados com sucesso'
            }
            
        except Exception as e:
            logger.error(f"Erro ao analisar arquivos do edital {edital_id}: {str(e)}")
            return {
                'edital_id': edital_id,
                'files_analyzed': 0,
                'error': str(e)
            }
    
    def analyze_all_pending_files(self) -> Dict:
        """
        Analisa todos os arquivos PDF que ainda não foram analisados
        
        Returns:
            Dict com estatísticas da análise
        """
        try:
            logger.info("Iniciando análise de todos os arquivos pendentes")
            
            # Buscar arquivos PDF sem análise
            pending_files = EditalFile.query.filter(
                EditalFile.file_type == 'PDF',
                EditalFile.extracted_text.is_(None)
            ).all()
            
            if not pending_files:
                return {
                    'total_pending': 0,
                    'analyzed': 0,
                    'errors': 0,
                    'message': 'Nenhum arquivo pendente de análise'
                }
            
            analyzed = 0
            errors = 0
            
            for pdf_file in pending_files:
                try:
                    if pdf_file.local_path and os.path.exists(pdf_file.local_path):
                        logger.info(f"Analisando: {pdf_file.filename}")
                        
                        analysis_result = self.analyzer.analyze_pdf(pdf_file.local_path)
                        self._save_analysis_to_database(pdf_file, analysis_result)
                        
                        analyzed += 1
                    else:
                        logger.warning(f"Arquivo não encontrado: {pdf_file.local_path}")
                        errors += 1
                        
                except Exception as e:
                    logger.error(f"Erro ao analisar {pdf_file.filename}: {str(e)}")
                    errors += 1
            
            # Commit das mudanças
            db.session.commit()
            
            return {
                'total_pending': len(pending_files),
                'analyzed': analyzed,
                'errors': errors,
                'message': f'{analyzed} arquivos analisados, {errors} erros'
            }
            
        except Exception as e:
            logger.error(f"Erro na análise em lote: {str(e)}")
            return {
                'total_pending': 0,
                'analyzed': 0,
                'errors': 1,
                'error': str(e)
            }
    
    def _save_analysis_to_database(self, pdf_file: EditalFile, analysis_result: Dict):
        """Salva resultado da análise no banco de dados"""
        try:
            # Extrair texto para busca
            text_preview = analysis_result.get('text_preview', '')
            
            # Preparar dados semânticos
            semantic_data = analysis_result.get('semantic_data', {})
            
            # Atualizar arquivo no banco
            pdf_file.extracted_text = text_preview
            pdf_file.semantic_data = json.dumps(semantic_data, ensure_ascii=False, default=str)
            
            # Atualizar informações do arquivo se disponíveis
            file_info = analysis_result.get('file_info', {})
            if file_info.get('size_bytes'):
                pdf_file.file_size = file_info['size_bytes']
            
            db.session.add(pdf_file)
            db.session.commit()
            
            logger.info(f"Análise salva para arquivo {pdf_file.filename}")
            
        except Exception as e:
            logger.error(f"Erro ao salvar análise no banco: {str(e)}")
            db.session.rollback()
    
    def _update_edital_with_analysis(self, edital: Edital, analysis_results: Dict):
        """Atualiza dados do edital com informações extraídas dos PDFs"""
        try:
            # Consolidar dados semânticos de todos os arquivos
            consolidated_data = {}
            
            for filename, result in analysis_results.items():
                semantic_data = result.get('semantic_data', {})
                
                # Priorizar dados mais específicos
                for key, value in semantic_data.items():
                    if value and (key not in consolidated_data or not consolidated_data[key]):
                        consolidated_data[key] = value
            
            # Atualizar campos do edital se não estiverem preenchidos
            if consolidated_data.get('numero_edital') and not edital.title:
                edital.title = f"Edital nº {consolidated_data['numero_edital']}"
            
            if consolidated_data.get('objeto') and not edital.object_description:
                edital.object_description = consolidated_data['objeto']
            
            if consolidated_data.get('modalidade') and not edital.modality:
                edital.modality = consolidated_data['modalidade'].title()
            
            if consolidated_data.get('valor_estimado') and not edital.estimated_value:
                try:
                    edital.estimated_value = float(consolidated_data['valor_estimado'])
                except:
                    pass
            
            # Salvar dados semânticos consolidados como JSON no campo description
            if consolidated_data:
                semantic_json = json.dumps(consolidated_data, ensure_ascii=False, indent=2, default=str)
                
                # Adicionar aos dados existentes
                if edital.description:
                    edital.description += f"\n\n--- DADOS EXTRAÍDOS DOS PDFs ---\n{semantic_json}"
                else:
                    edital.description = f"--- DADOS EXTRAÍDOS DOS PDFs ---\n{semantic_json}"
            
            db.session.add(edital)
            db.session.commit()
            
            logger.info(f"Edital {edital.id} atualizado com dados extraídos dos PDFs")
            
        except Exception as e:
            logger.error(f"Erro ao atualizar edital com análise: {str(e)}")
            db.session.rollback()
    
    def get_analysis_summary(self, edital_id: int) -> Dict:
        """
        Retorna resumo da análise de PDFs de um edital
        
        Args:
            edital_id: ID do edital
            
        Returns:
            Dict com resumo da análise
        """
        try:
            # Buscar arquivos analisados
            analyzed_files = EditalFile.query.filter(
                EditalFile.edital_id == edital_id,
                EditalFile.file_type == 'PDF',
                EditalFile.extracted_text.isnot(None)
            ).all()
            
            if not analyzed_files:
                return {
                    'edital_id': edital_id,
                    'analyzed_files': 0,
                    'message': 'Nenhum arquivo analisado'
                }
            
            summary = {
                'edital_id': edital_id,
                'analyzed_files': len(analyzed_files),
                'files': []
            }
            
            for file in analyzed_files:
                file_summary = {
                    'filename': file.filename,
                    'file_size': file.file_size,
                    'text_length': len(file.extracted_text) if file.extracted_text else 0,
                    'has_semantic_data': bool(file.semantic_data),
                    'created_at': file.created_at.isoformat() if file.created_at else None
                }
                
                # Adicionar dados semânticos se disponíveis
                if file.semantic_data:
                    try:
                        semantic_data = json.loads(file.semantic_data)
                        file_summary['semantic_fields'] = list(semantic_data.keys())
                        file_summary['key_data'] = {
                            k: v for k, v in semantic_data.items() 
                            if k in ['numero_edital', 'modalidade', 'objeto', 'valor_estimado']
                        }
                    except:
                        pass
                
                summary['files'].append(file_summary)
            
            return summary
            
        except Exception as e:
            logger.error(f"Erro ao obter resumo da análise: {str(e)}")
            return {
                'edital_id': edital_id,
                'analyzed_files': 0,
                'error': str(e)
            }
    
    def search_in_pdf_content(self, query: str, limit: int = 10) -> List[Dict]:
        """
        Busca por texto no conteúdo extraído dos PDFs
        
        Args:
            query: Texto a buscar
            limit: Limite de resultados
            
        Returns:
            Lista de arquivos que contêm o texto
        """
        try:
            # Buscar arquivos que contêm o texto
            files = EditalFile.query.filter(
                EditalFile.extracted_text.ilike(f'%{query}%')
            ).limit(limit).all()
            
            results = []
            for file in files:
                # Encontrar contexto do texto
                text = file.extracted_text or ''
                query_lower = query.lower()
                text_lower = text.lower()
                
                index = text_lower.find(query_lower)
                if index >= 0:
                    # Extrair contexto (100 chars antes e depois)
                    start = max(0, index - 100)
                    end = min(len(text), index + len(query) + 100)
                    context = text[start:end]
                    
                    results.append({
                        'file_id': file.id,
                        'edital_id': file.edital_id,
                        'filename': file.filename,
                        'context': context,
                        'match_position': index
                    })
            
            return results
            
        except Exception as e:
            logger.error(f"Erro na busca em PDFs: {str(e)}")
            return []

