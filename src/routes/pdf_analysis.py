from flask import Blueprint, request, jsonify, current_app
from src.services.pdf_integration import PDFIntegrationService
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

pdf_analysis_bp = Blueprint('pdf_analysis', __name__)

@pdf_analysis_bp.route('/editais/<int:edital_id>/analyze-pdfs', methods=['POST'])
def analyze_edital_pdfs(edital_id):
    """
    POST /api/editais/{id}/analyze-pdfs
    Analisa todos os arquivos PDF de um edital específico
    """
    try:
        logger.info(f"Iniciando análise de PDFs do edital {edital_id}")
        
        service = PDFIntegrationService()
        result = service.analyze_edital_files(edital_id)
        
        if 'error' in result:
            return jsonify({
                'success': False,
                'error': result['error'],
                'edital_id': edital_id,
                'timestamp': datetime.now().isoformat()
            }), 500
        
        return jsonify({
            'success': True,
            'message': result['message'],
            'edital_id': edital_id,
            'files_analyzed': result['files_analyzed'],
            'analysis_summary': {
                'total_files': result['files_analyzed'],
                'has_results': len(result.get('results', {})) > 0
            },
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Erro ao analisar PDFs do edital {edital_id}: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e),
            'edital_id': edital_id,
            'timestamp': datetime.now().isoformat()
        }), 500

@pdf_analysis_bp.route('/editais/analyze-all-pdfs', methods=['POST'])
def analyze_all_pending_pdfs():
    """
    POST /api/editais/analyze-all-pdfs
    Analisa todos os arquivos PDF pendentes de análise
    """
    try:
        logger.info("Iniciando análise de todos os PDFs pendentes")
        
        service = PDFIntegrationService()
        result = service.analyze_all_pending_files()
        
        if 'error' in result:
            return jsonify({
                'success': False,
                'error': result['error'],
                'timestamp': datetime.now().isoformat()
            }), 500
        
        return jsonify({
            'success': True,
            'message': result['message'],
            'statistics': {
                'total_pending': result['total_pending'],
                'analyzed': result['analyzed'],
                'errors': result['errors']
            },
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Erro na análise em lote de PDFs: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@pdf_analysis_bp.route('/editais/<int:edital_id>/pdf-analysis-summary', methods=['GET'])
def get_pdf_analysis_summary(edital_id):
    """
    GET /api/editais/{id}/pdf-analysis-summary
    Retorna resumo da análise de PDFs de um edital
    """
    try:
        service = PDFIntegrationService()
        summary = service.get_analysis_summary(edital_id)
        
        if 'error' in summary:
            return jsonify({
                'error': summary['error'],
                'edital_id': edital_id,
                'timestamp': datetime.now().isoformat()
            }), 500
        
        return jsonify({
            'edital_id': edital_id,
            'summary': summary,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Erro ao obter resumo da análise: {str(e)}")
        return jsonify({
            'error': str(e),
            'edital_id': edital_id,
            'timestamp': datetime.now().isoformat()
        }), 500

@pdf_analysis_bp.route('/editais/search-in-pdfs', methods=['GET'])
def search_in_pdf_content():
    """
    GET /api/editais/search-in-pdfs?q=texto&limit=10
    Busca por texto no conteúdo extraído dos PDFs
    """
    try:
        query = request.args.get('q', '').strip()
        limit = min(int(request.args.get('limit', 10)), 50)
        
        if not query:
            return jsonify({
                'error': 'Parâmetro "q" (query) é obrigatório',
                'timestamp': datetime.now().isoformat()
            }), 400
        
        if len(query) < 3:
            return jsonify({
                'error': 'Query deve ter pelo menos 3 caracteres',
                'timestamp': datetime.now().isoformat()
            }), 400
        
        service = PDFIntegrationService()
        results = service.search_in_pdf_content(query, limit)
        
        return jsonify({
            'query': query,
            'limit': limit,
            'total_results': len(results),
            'results': results,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Erro na busca em PDFs: {str(e)}")
        return jsonify({
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@pdf_analysis_bp.route('/editais/pdf-analysis-stats', methods=['GET'])
def get_pdf_analysis_stats():
    """
    GET /api/editais/pdf-analysis-stats
    Retorna estatísticas gerais da análise de PDFs
    """
    try:
        from src.models.edital import EditalFile
        from src.models.user import db
        from sqlalchemy import func
        
        # Estatísticas de arquivos
        total_files = EditalFile.query.filter_by(file_type='PDF').count()
        analyzed_files = EditalFile.query.filter(
            EditalFile.file_type == 'PDF',
            EditalFile.extracted_text.isnot(None)
        ).count()
        pending_files = total_files - analyzed_files
        
        # Tamanho total dos arquivos analisados
        total_size = db.session.query(
            func.sum(EditalFile.file_size)
        ).filter(
            EditalFile.file_type == 'PDF',
            EditalFile.extracted_text.isnot(None)
        ).scalar() or 0
        
        # Arquivos com dados semânticos
        files_with_semantic = EditalFile.query.filter(
            EditalFile.file_type == 'PDF',
            EditalFile.semantic_data.isnot(None)
        ).count()
        
        return jsonify({
            'statistics': {
                'total_pdf_files': total_files,
                'analyzed_files': analyzed_files,
                'pending_analysis': pending_files,
                'files_with_semantic_data': files_with_semantic,
                'total_size_mb': round(total_size / (1024 * 1024), 2) if total_size else 0,
                'analysis_coverage': round((analyzed_files / total_files * 100), 1) if total_files > 0 else 0
            },
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Erro ao obter estatísticas de análise: {str(e)}")
        return jsonify({
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@pdf_analysis_bp.route('/editais/<int:edital_id>/pdf-content', methods=['GET'])
def get_pdf_content(edital_id):
    """
    GET /api/editais/{id}/pdf-content
    Retorna o conteúdo extraído dos PDFs de um edital
    """
    try:
        from src.models.edital import EditalFile
        
        files = EditalFile.query.filter(
            EditalFile.edital_id == edital_id,
            EditalFile.file_type == 'PDF',
            EditalFile.extracted_text.isnot(None)
        ).all()
        
        if not files:
            return jsonify({
                'edital_id': edital_id,
                'message': 'Nenhum conteúdo de PDF encontrado',
                'files': [],
                'timestamp': datetime.now().isoformat()
            })
        
        content = []
        for file in files:
            file_content = {
                'file_id': file.id,
                'filename': file.filename,
                'file_size': file.file_size,
                'extracted_text': file.extracted_text,
                'text_length': len(file.extracted_text) if file.extracted_text else 0
            }
            
            # Adicionar dados semânticos se disponíveis
            if file.semantic_data:
                try:
                    import json
                    file_content['semantic_data'] = json.loads(file.semantic_data)
                except:
                    file_content['semantic_data'] = None
            
            content.append(file_content)
        
        return jsonify({
            'edital_id': edital_id,
            'total_files': len(content),
            'files': content,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Erro ao obter conteúdo dos PDFs: {str(e)}")
        return jsonify({
            'error': str(e),
            'edital_id': edital_id,
            'timestamp': datetime.now().isoformat()
        }), 500

