from flask import Blueprint, request, jsonify, current_app
from src.models.user import db
from src.models.edital import Edital, EditalItem, EditalFile
from sqlalchemy import and_, or_, func, desc, asc
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

edital_advanced_bp = Blueprint('edital_advanced', __name__)

@edital_advanced_bp.route('/editais/search', methods=['GET'])
def search_editais():
    """
    GET /api/editais/search?q=material&uf=SP&modalidade=pregao&valor_min=1000&valor_max=50000
    Busca avançada de editais com múltiplos filtros
    """
    try:
        # Parâmetros de busca
        query_text = request.args.get('q', '').strip()
        uf = request.args.get('uf', '').strip()
        modalidade = request.args.get('modalidade', '').strip()
        organization = request.args.get('organization', '').strip()
        municipality = request.args.get('municipality', '').strip()
        
        # Filtros de valor
        valor_min = request.args.get('valor_min', type=float)
        valor_max = request.args.get('valor_max', type=float)
        
        # Filtros de data
        data_inicio = request.args.get('data_inicio')  # YYYY-MM-DD
        data_fim = request.args.get('data_fim')  # YYYY-MM-DD
        
        # Filtros booleanos
        tem_botao_acesso = request.args.get('tem_botao_acesso', type=bool)
        tem_arquivos = request.args.get('tem_arquivos', type=bool)
        
        # Parâmetros de ordenação
        order_by = request.args.get('order_by', 'created_at')  # created_at, estimated_value, publication_date
        order_dir = request.args.get('order_dir', 'desc')  # asc, desc
        
        # Parâmetros de paginação
        page = int(request.args.get('page', 1))
        per_page = min(int(request.args.get('per_page', 10)), 100)  # Máximo 100 por página
        
        # Construir query base
        query = Edital.query
        
        # Filtro de texto (busca em título, descrição e objeto)
        if query_text:
            text_filter = or_(
                Edital.title.ilike(f'%{query_text}%'),
                Edital.description.ilike(f'%{query_text}%'),
                Edital.object_description.ilike(f'%{query_text}%'),
                Edital.organization_name.ilike(f'%{query_text}%')
            )
            query = query.filter(text_filter)
        
        # Filtros específicos
        if uf:
            query = query.filter(Edital.state_code.ilike(f'%{uf}%'))
        
        if modalidade:
            query = query.filter(Edital.modality.ilike(f'%{modalidade}%'))
            
        if organization:
            query = query.filter(Edital.organization_name.ilike(f'%{organization}%'))
            
        if municipality:
            query = query.filter(Edital.municipality_name.ilike(f'%{municipality}%'))
        
        # Filtros de valor
        if valor_min is not None:
            query = query.filter(Edital.estimated_value >= valor_min)
        
        if valor_max is not None:
            query = query.filter(Edital.estimated_value <= valor_max)
        
        # Filtros de data
        if data_inicio:
            try:
                data_inicio_obj = datetime.strptime(data_inicio, '%Y-%m-%d').date()
                query = query.filter(Edital.publication_date >= data_inicio_obj)
            except ValueError:
                pass
        
        if data_fim:
            try:
                data_fim_obj = datetime.strptime(data_fim, '%Y-%m-%d').date()
                query = query.filter(Edital.publication_date <= data_fim_obj)
            except ValueError:
                pass
        
        # Filtros booleanos
        if tem_botao_acesso is not None:
            query = query.filter(Edital.has_access_button == tem_botao_acesso)
        
        if tem_arquivos is not None:
            if tem_arquivos:
                # Editais que têm arquivos
                query = query.join(EditalFile).filter(EditalFile.id.isnot(None))
            else:
                # Editais que não têm arquivos
                query = query.outerjoin(EditalFile).filter(EditalFile.id.is_(None))
        
        # Ordenação
        if order_by == 'estimated_value':
            order_column = Edital.estimated_value
        elif order_by == 'publication_date':
            order_column = Edital.publication_date
        else:
            order_column = Edital.created_at
        
        if order_dir == 'asc':
            query = query.order_by(asc(order_column))
        else:
            query = query.order_by(desc(order_column))
        
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
            },
            'filters_applied': {
                'query_text': query_text,
                'uf': uf,
                'modalidade': modalidade,
                'organization': organization,
                'municipality': municipality,
                'valor_min': valor_min,
                'valor_max': valor_max,
                'data_inicio': data_inicio,
                'data_fim': data_fim,
                'tem_botao_acesso': tem_botao_acesso,
                'tem_arquivos': tem_arquivos,
                'order_by': order_by,
                'order_dir': order_dir
            }
        })
        
    except Exception as e:
        logger.error(f"Erro na busca avançada: {str(e)}")
        return jsonify({'error': str(e)}), 500

@edital_advanced_bp.route('/editais/filtros', methods=['GET'])
def get_filtros_disponiveis():
    """
    GET /api/editais/filtros
    Retorna opções disponíveis para filtros (UFs, modalidades, organizações)
    """
    try:
        # UFs disponíveis
        ufs = db.session.query(
            Edital.state_code,
            func.count(Edital.id).label('count')
        ).filter(
            Edital.state_code.isnot(None),
            Edital.state_code != ''
        ).group_by(Edital.state_code).order_by(Edital.state_code).all()
        
        # Modalidades disponíveis
        modalidades = db.session.query(
            Edital.modality,
            func.count(Edital.id).label('count')
        ).filter(
            Edital.modality.isnot(None),
            Edital.modality != ''
        ).group_by(Edital.modality).order_by(func.count(Edital.id).desc()).all()
        
        # Organizações mais frequentes (top 20)
        organizacoes = db.session.query(
            Edital.organization_name,
            func.count(Edital.id).label('count')
        ).filter(
            Edital.organization_name.isnot(None),
            Edital.organization_name != ''
        ).group_by(Edital.organization_name).order_by(
            func.count(Edital.id).desc()
        ).limit(20).all()
        
        # Municípios mais frequentes (top 30)
        municipios = db.session.query(
            Edital.municipality_name,
            Edital.state_code,
            func.count(Edital.id).label('count')
        ).filter(
            Edital.municipality_name.isnot(None),
            Edital.municipality_name != ''
        ).group_by(
            Edital.municipality_name, 
            Edital.state_code
        ).order_by(
            func.count(Edital.id).desc()
        ).limit(30).all()
        
        # Faixas de valor
        valor_stats = db.session.query(
            func.min(Edital.estimated_value).label('valor_min'),
            func.max(Edital.estimated_value).label('valor_max'),
            func.avg(Edital.estimated_value).label('valor_medio'),
            func.count(Edital.id).filter(Edital.estimated_value.isnot(None)).label('com_valor')
        ).first()
        
        return jsonify({
            'ufs': [{'uf': uf, 'count': count} for uf, count in ufs],
            'modalidades': [{'modalidade': mod, 'count': count} for mod, count in modalidades],
            'organizacoes': [{'organizacao': org, 'count': count} for org, count in organizacoes],
            'municipios': [
                {'municipio': mun, 'uf': uf, 'count': count} 
                for mun, uf, count in municipios
            ],
            'valores': {
                'min': float(valor_stats.valor_min) if valor_stats.valor_min else 0,
                'max': float(valor_stats.valor_max) if valor_stats.valor_max else 0,
                'medio': float(valor_stats.valor_medio) if valor_stats.valor_medio else 0,
                'com_valor': valor_stats.com_valor or 0
            },
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Erro ao obter filtros: {str(e)}")
        return jsonify({'error': str(e)}), 500

@edital_advanced_bp.route('/editais/recentes', methods=['GET'])
def get_editais_recentes():
    """
    GET /api/editais/recentes?dias=7&limit=10
    Retorna editais mais recentes (por data de criação ou publicação)
    """
    try:
        dias = int(request.args.get('dias', 7))
        limit = min(int(request.args.get('limit', 10)), 50)
        
        # Data limite
        data_limite = datetime.now() - timedelta(days=dias)
        
        # Buscar editais recentes
        editais = Edital.query.filter(
            or_(
                Edital.created_at >= data_limite,
                Edital.publication_date >= data_limite.date()
            )
        ).order_by(desc(Edital.created_at)).limit(limit).all()
        
        return jsonify({
            'editais': [edital.to_dict() for edital in editais],
            'filtros': {
                'dias': dias,
                'limit': limit,
                'data_limite': data_limite.isoformat()
            },
            'total': len(editais),
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Erro ao buscar editais recentes: {str(e)}")
        return jsonify({'error': str(e)}), 500

@edital_advanced_bp.route('/editais/por-valor', methods=['GET'])
def get_editais_por_valor():
    """
    GET /api/editais/por-valor?ordem=desc&limit=20
    Retorna editais ordenados por valor estimado
    """
    try:
        ordem = request.args.get('ordem', 'desc')  # desc ou asc
        limit = min(int(request.args.get('limit', 20)), 100)
        
        # Buscar editais com valor
        query = Edital.query.filter(
            Edital.estimated_value.isnot(None),
            Edital.estimated_value > 0
        )
        
        if ordem == 'asc':
            query = query.order_by(asc(Edital.estimated_value))
        else:
            query = query.order_by(desc(Edital.estimated_value))
        
        editais = query.limit(limit).all()
        
        return jsonify({
            'editais': [edital.to_dict() for edital in editais],
            'filtros': {
                'ordem': ordem,
                'limit': limit
            },
            'total': len(editais),
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Erro ao buscar editais por valor: {str(e)}")
        return jsonify({'error': str(e)}), 500

@edital_advanced_bp.route('/editais/<int:edital_id>/items', methods=['GET'])
def get_edital_items(edital_id):
    """
    GET /api/editais/123/items
    Retorna apenas os itens de um edital específico
    """
    try:
        edital = Edital.query.get_or_404(edital_id)
        
        items = EditalItem.query.filter_by(edital_id=edital_id).order_by(
            EditalItem.numero
        ).all()
        
        return jsonify({
            'edital': {
                'id': edital.id,
                'title': edital.title,
                'organization_name': edital.organization_name,
                'municipality_name': edital.municipality_name,
                'state_code': edital.state_code
            },
            'items': [item.to_dict() for item in items],
            'total_items': len(items),
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Erro ao buscar itens do edital {edital_id}: {str(e)}")
        return jsonify({'error': str(e)}), 500

@edital_advanced_bp.route('/editais/<int:edital_id>/files', methods=['GET'])
def get_edital_files(edital_id):
    """
    GET /api/editais/123/files
    Retorna apenas os arquivos de um edital específico
    """
    try:
        edital = Edital.query.get_or_404(edital_id)
        
        files = EditalFile.query.filter_by(edital_id=edital_id).order_by(
            EditalFile.created_at
        ).all()
        
        return jsonify({
            'edital': {
                'id': edital.id,
                'title': edital.title,
                'organization_name': edital.organization_name,
                'municipality_name': edital.municipality_name,
                'state_code': edital.state_code
            },
            'files': [file.to_dict() for file in files],
            'total_files': len(files),
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Erro ao buscar arquivos do edital {edital_id}: {str(e)}")
        return jsonify({'error': str(e)}), 500

@edital_advanced_bp.route('/editais/bulk-scrape', methods=['POST'])
def bulk_scrape():
    """
    POST /api/editais/bulk-scrape
    Body: {"estados": ["SP", "RJ", "MG"], "limit_per_state": 5}
    Executa scraping em lote para múltiplos estados
    """
    import asyncio
    from src.services.scraper_integration import ScraperIntegrationService
    
    try:
        data = request.get_json() or {}
        estados = data.get('estados', ['SP'])
        limit_per_state = data.get('limit_per_state', 3)
        
        if len(estados) > 5:
            return jsonify({
                'error': 'Máximo de 5 estados por requisição'
            }), 400
        
        logger.info(f"Iniciando scraping em lote para estados: {estados}")
        
        # Obter serviço de integração
        scraper_service = ScraperIntegrationService()
        scraper_service.init_app(current_app)
        
        # Executar scraping assíncrono
        async def run_bulk_scraping():
            return await scraper_service.run_scraping(
                estados=estados, 
                limit=limit_per_state
            )
        
        stats = asyncio.run(run_bulk_scraping())
        
        return jsonify({
            'success': True,
            'message': f'Scraping em lote executado para {len(estados)} estados',
            'estados_processados': estados,
            'limit_per_state': limit_per_state,
            'stats': stats,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Erro no scraping em lote: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

