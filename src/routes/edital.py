from flask import Blueprint, request, jsonify, current_app, send_file
from src.models.user import db
from src.models.edital import Edital, EditalItem, EditalFile
from src.services.scraper_integration import ScraperIntegrationService
import logging
import os
from datetime import datetime

logger = logging.getLogger(__name__)

edital_bp = Blueprint('edital', __name__)

@edital_bp.route('/editais', methods=['GET'])
def get_editais():
    """
    GET /api/editais?uf=SP&modalidade=pregao&page=1&per_page=10
    Retorna lista de editais com filtros
    """
    try:
        # Parâmetros de filtro
        uf = request.args.get('uf')
        modalidade = request.args.get('modalidade')
        organization = request.args.get('organization')
        municipality = request.args.get('municipality')
        
        # Parâmetros de paginação
        page = int(request.args.get('page', 1))
        per_page = int(request.args.get('per_page', 10))
        
        # Construir query
        query = Edital.query
        
        if uf:
            query = query.filter(Edital.state_code.ilike(f'%{uf}%'))
        
        if modalidade:
            query = query.filter(Edital.modality.ilike(f'%{modalidade}%'))
            
        if organization:
            query = query.filter(Edital.organization_name.ilike(f'%{organization}%'))
            
        if municipality:
            query = query.filter(Edital.municipality_name.ilike(f'%{municipality}%'))
        
        # Ordenar por data de criação (mais recentes primeiro)
        query = query.order_by(Edital.created_at.desc())
        
        # Paginar
        pagination = query.paginate(
            page=page, 
            per_page=per_page, 
            error_out=False
        )
        
        editais = pagination.items
        
        return jsonify({
            'editais': [edital.to_dict() for edital in editais],
            'pagination': {
                'page': page,
                'per_page': per_page,
                'total': pagination.total,
                'pages': pagination.pages,
                'has_next': pagination.has_next,
                'has_prev': pagination.has_prev
            }
        })
        
    except Exception as e:
        logger.error(f"Erro ao buscar editais: {str(e)}")
        return jsonify({'error': str(e)}), 500

@edital_bp.route('/editais/<int:edital_id>', methods=['GET'])
def get_edital_details(edital_id):
    """
    GET /api/editais/123
    Retorna detalhes completos de um edital
    """
    try:
        edital = Edital.query.get_or_404(edital_id)
        return jsonify(edital.to_dict())
        
    except Exception as e:
        logger.error(f"Erro ao buscar edital {edital_id}: {str(e)}")
        return jsonify({'error': str(e)}), 500

@edital_bp.route('/editais/<int:edital_id>/download/<int:file_id>', methods=['GET'])
def download_edital_file(edital_id, file_id):
    """
    GET /api/editais/123/download/456
    Faz download de um arquivo do edital
    """
    try:
        edital_file = EditalFile.query.filter_by(
            id=file_id, 
            edital_id=edital_id
        ).first_or_404()
        
        if not edital_file.local_path or not os.path.exists(edital_file.local_path):
            return jsonify({'error': 'Arquivo não encontrado no servidor'}), 404
        
        return send_file(
            edital_file.local_path,
            as_attachment=True,
            download_name=edital_file.filename
        )
        
    except Exception as e:
        logger.error(f"Erro ao fazer download do arquivo {file_id}: {str(e)}")
        return jsonify({'error': str(e)}), 500

@edital_bp.route('/editais/scrape', methods=['POST'])
def run_scraping():
    """
    POST /api/editais/scrape
    Body: {"estados": ["SP", "RJ"], "limit": 5}
    Executa coleta de editais
    """
    try:
        data = request.get_json() or {}
        estados = data.get('estados', ['SP'])
        limit = data.get('limit', 3)  # Limite padrão para teste
        
        logger.info(f"Iniciando scraping para estados: {estados}, limit: {limit}")
        
        # Obter serviço de integração
        scraper_service = ScraperIntegrationService()
        scraper_service.init_app(current_app)
        
        # Executar scraping
        stats = scraper_service.run_scraping(estados=estados, limit=limit)
        
        return jsonify({
            'success': True,
            'message': 'Scraping executado com sucesso',
            'stats': stats,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Erro no scraping: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@edital_bp.route('/editais/upload-json', methods=['POST'])
def upload_json():
    """
    POST /api/editais/upload-json
    Body: {"json_path": "/path/to/file.json"}
    Carrega dados de um arquivo JSON
    """
    try:
        data = request.get_json()
        json_path = data.get('json_path')
        
        if not json_path or not os.path.exists(json_path):
            return jsonify({'error': 'Arquivo JSON não encontrado'}), 400
        
        # Obter serviço de integração
        scraper_service = ScraperIntegrationService()
        scraper_service.init_app(current_app)
        
        # Carregar JSON
        stats = scraper_service.load_json_to_database(json_path)
        
        return jsonify({
            'success': True,
            'message': 'JSON carregado com sucesso',
            'stats': stats,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Erro ao carregar JSON: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@edital_bp.route('/editais/stats', methods=['GET'])
def get_stats():
    """
    GET /api/editais/stats
    Retorna estatísticas gerais dos editais
    """
    try:
        total_editais = Edital.query.count()
        total_items = EditalItem.query.count()
        total_files = EditalFile.query.count()
        
        # Estatísticas por estado
        estados_stats = db.session.query(
            Edital.state_code,
            db.func.count(Edital.id).label('count')
        ).group_by(Edital.state_code).all()
        
        # Estatísticas por modalidade
        modalidade_stats = db.session.query(
            Edital.modality,
            db.func.count(Edital.id).label('count')
        ).group_by(Edital.modality).all()
        
        # Valor total estimado
        valor_total = db.session.query(
            db.func.sum(Edital.estimated_value)
        ).scalar() or 0
        
        return jsonify({
            'total_editais': total_editais,
            'total_items': total_items,
            'total_files': total_files,
            'valor_total_estimado': float(valor_total),
            'por_estado': [{'uf': uf, 'count': count} for uf, count in estados_stats],
            'por_modalidade': [{'modalidade': mod, 'count': count} for mod, count in modalidade_stats],
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Erro ao obter estatísticas: {str(e)}")
        return jsonify({'error': str(e)}), 500

@edital_bp.route('/editais/health', methods=['GET'])
def health_check():
    """
    GET /api/editais/health
    Health check do sistema de editais
    """
    try:
        # Verificar conexão com banco
        total_editais = Edital.query.count()
        
        return jsonify({
            'status': 'healthy',
            'database': 'connected',
            'total_editais': total_editais,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Erro no health check: {str(e)}")
        return jsonify({
            'status': 'unhealthy',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

