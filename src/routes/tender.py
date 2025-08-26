from flask import Blueprint, request, jsonify, send_file, redirect
from datetime import datetime, date
from sqlalchemy import and_, or_
import logging

from src.models.user import db
from src.models.tender import Tender, City
from src.services.data_scraper import DataScraper

logger = logging.getLogger(__name__)

tender_bp = Blueprint('tender', __name__)

@tender_bp.route('/cities', methods=['GET'])
def get_cities():
    """Get list of available cities"""
    try:
        cities = City.query.all()
        return jsonify({
            'success': True,
            'cities': [city.to_dict() for city in cities]
        })
    except Exception as e:
        logger.error(f"Error fetching cities: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@tender_bp.route('/cities/<ibge_code>', methods=['GET'])
def get_city(ibge_code):
    """Get specific city by IBGE code"""
    try:
        city = City.query.filter_by(ibge_code=ibge_code).first()
        if not city:
            return jsonify({
                'success': False,
                'error': 'City not found'
            }), 404
        
        return jsonify({
            'success': True,
            'city': city.to_dict()
        })
    except Exception as e:
        logger.error(f"Error fetching city {ibge_code}: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@tender_bp.route('/tenders', methods=['GET'])
def search_tenders():
    """Search for tenders with filters"""
    try:
        # Get query parameters
        city_ibge = request.args.get('city_ibge')
        status = request.args.get('status')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        keyword = request.args.get('keyword')
        data_source = request.args.get('data_source')
        page = int(request.args.get('page', 1))
        per_page = min(int(request.args.get('per_page', 20)), 100)  # Max 100 per page
        
        # Build query
        query = Tender.query
        
        # Apply filters
        if city_ibge:
            query = query.filter(Tender.municipality_ibge == city_ibge)
        
        if status:
            query = query.filter(Tender.status.ilike(f'%{status}%'))
        
        if start_date:
            try:
                start_date_obj = datetime.strptime(start_date, '%Y-%m-%d').date()
                query = query.filter(Tender.publication_date >= start_date_obj)
            except ValueError:
                return jsonify({
                    'success': False,
                    'error': 'Invalid start_date format. Use YYYY-MM-DD'
                }), 400
        
        if end_date:
            try:
                end_date_obj = datetime.strptime(end_date, '%Y-%m-%d').date()
                query = query.filter(Tender.publication_date <= end_date_obj)
            except ValueError:
                return jsonify({
                    'success': False,
                    'error': 'Invalid end_date format. Use YYYY-MM-DD'
                }), 400
        
        if keyword:
            keyword_filter = or_(
                Tender.title.ilike(f'%{keyword}%'),
                Tender.description.ilike(f'%{keyword}%'),
                Tender.organization_name.ilike(f'%{keyword}%')
            )
            query = query.filter(keyword_filter)
        
        if data_source:
            query = query.filter(Tender.data_source == data_source.upper())
        
        # Order by publication date (most recent first)
        query = query.order_by(Tender.publication_date.desc(), Tender.created_at.desc())
        
        # Paginate
        pagination = query.paginate(
            page=page,
            per_page=per_page,
            error_out=False
        )
        
        tenders = pagination.items
        
        return jsonify({
            'success': True,
            'tenders': [tender.to_dict() for tender in tenders],
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
        logger.error(f"Error searching tenders: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@tender_bp.route('/tenders/<int:tender_id>', methods=['GET'])
def get_tender(tender_id):
    """Get specific tender by ID"""
    try:
        tender = Tender.query.get(tender_id)
        if not tender:
            return jsonify({
                'success': False,
                'error': 'Tender not found'
            }), 404
        
        return jsonify({
            'success': True,
            'tender': tender.to_dict()
        })
    except Exception as e:
        logger.error(f"Error fetching tender {tender_id}: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@tender_bp.route('/stats', methods=['GET'])
def get_stats():
    """Get statistics about tenders"""
    try:
        scraper = DataScraper()
        stats = scraper.get_scraping_stats()
        
        return jsonify({
            'success': True,
            'stats': stats
        })
    except Exception as e:
        logger.error(f"Error fetching stats: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@tender_bp.route('/scrape', methods=['POST'])
def run_scraping():
    """Manually trigger data scraping"""
    try:
        scraper = DataScraper()
        results = scraper.run_full_scraping()
        
        return jsonify({
            'success': True,
            'results': results
        })
    except Exception as e:
        logger.error(f"Error running scraping: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@tender_bp.route('/tenders/<int:tender_id>/pdf', methods=['GET'])
def get_tender_pdf(tender_id):
    """Serve PDF file for a specific tender"""
    try:
        tender = Tender.query.get(tender_id)
        if not tender:
            return jsonify({
                'success': False,
                'error': 'Tender not found'
            }), 404
        
        # Verificar se tem arquivos baixados
        if not tender.downloaded_files or len(tender.downloaded_files) == 0:
            return jsonify({
                'success': False,
                'error': 'No PDF files available for this tender'
            }), 404
        
        # Pegar o primeiro arquivo PDF
        pdf_file = tender.downloaded_files[0]
        
        # Verificar se é um caminho local ou URL
        if 'local_path' in pdf_file and pdf_file['local_path']:
            # Arquivo local - servir diretamente
            file_path = pdf_file['local_path']
            if os.path.exists(file_path):
                return send_file(
                    file_path,
                    as_attachment=False,
                    download_name=pdf_file.get('filename', 'edital.pdf'),
                    mimetype='application/pdf'
                )
            else:
                return jsonify({
                    'success': False,
                    'error': 'PDF file not found on server'
                }), 404
        
        elif 'url' in pdf_file and pdf_file['url']:
            # URL externa - redirecionar
            return redirect(pdf_file['url'])
        
        else:
            return jsonify({
                'success': False,
                'error': 'No valid PDF path or URL found'
            }), 404
            
    except Exception as e:
        logger.error(f"Error serving PDF for tender {tender_id}: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@tender_bp.route('/tenders/<int:tender_id>/files', methods=['GET'])
def get_tender_files(tender_id):
    """Get all files for a specific tender"""
    try:
        tender = Tender.query.get(tender_id)
        if not tender:
            return jsonify({
                'success': False,
                'error': 'Tender not found'
            }), 404
        
        files = tender.downloaded_files or []
        
        # Processar arquivos para adicionar informações úteis
        processed_files = []
        for file_info in files:
            processed_file = {
                'filename': file_info.get('filename', 'Arquivo'),
                'url': file_info.get('url', ''),
                'local_path': file_info.get('local_path', ''),
                'file_size': file_info.get('file_size', 0),
                'file_type': file_info.get('file_type', 'PDF'),
                'download_url': f"/api/tenders/{tender_id}/pdf" if file_info.get('local_path') else file_info.get('url', ''),
                'is_local': bool(file_info.get('local_path')),
                'is_available': bool(file_info.get('local_path') and os.path.exists(file_info['local_path'])) or bool(file_info.get('url'))
            }
            processed_files.append(processed_file)
        
        return jsonify({
            'success': True,
            'files': processed_files,
            'total_files': len(processed_files)
        })
        
    except Exception as e:
        logger.error(f"Error fetching files for tender {tender_id}: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@tender_bp.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    try:
        # Check database connection
        from sqlalchemy import text
        db.session.execute(text('SELECT 1'))
        
        # Get basic stats
        total_tenders = Tender.query.count()
        total_cities = City.query.count()
        
        return jsonify({
            'success': True,
            'status': 'healthy',
            'database': 'connected',
            'total_tenders': total_tenders,
            'total_cities': total_cities,
            'timestamp': datetime.utcnow().isoformat()
        })
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return jsonify({
            'success': False,
            'status': 'unhealthy',
            'error': str(e),
            'timestamp': datetime.utcnow().isoformat()
        }), 500

