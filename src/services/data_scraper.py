import logging
from datetime import datetime, date
from typing import List, Dict, Optional
from sqlalchemy.exc import IntegrityError

from src.services.pncp_client import PNCPClient
from src.services.querido_diario_client import QueridoDiarioClient
from src.models.user import db
from src.models.tender import Tender, City

logger = logging.getLogger(__name__)

class DataScraper:
    """Main data scraper service that coordinates PNCP and Querido Diário APIs"""
    
    def __init__(self):
        self.pncp_client = PNCPClient()
        self.qd_client = QueridoDiarioClient()
        
        # Target cities for MVP
        self.target_cities = [
            {'ibge_code': '3526902', 'name': 'Limeira', 'state_code': 'SP', 'state_name': 'São Paulo'},
            {'ibge_code': '3509502', 'name': 'Campinas', 'state_code': 'SP', 'state_name': 'São Paulo'},
            {'ibge_code': '2927408', 'name': 'Salvador', 'state_code': 'BA', 'state_name': 'Bahia'}
        ]
    
    def initialize_cities(self):
        """Initialize target cities in the database"""
        try:
            logger.info("Initializing target cities in database")
            
            for city_data in self.target_cities:
                # Check if city already exists
                existing_city = City.query.filter_by(ibge_code=city_data['ibge_code']).first()
                
                if not existing_city:
                    city = City(
                        ibge_code=city_data['ibge_code'],
                        name=city_data['name'],
                        state_code=city_data['state_code'],
                        state_name=city_data['state_name']
                    )
                    db.session.add(city)
                    logger.info(f"Added city: {city_data['name']}")
                else:
                    logger.info(f"City already exists: {city_data['name']}")
            
            db.session.commit()
            logger.info("Cities initialization completed")
            
        except Exception as e:
            logger.error(f"Error initializing cities: {e}")
            db.session.rollback()
            raise
    
    def save_tender_to_db(self, tender_data: Dict) -> Optional[Tender]:
        """
        Save tender data to database
        
        Args:
            tender_data: Parsed tender data
            
        Returns:
            Tender object if saved successfully, None otherwise
        """
        try:
            # Check if tender already exists (avoid duplicates)
            existing_tender = None
            if tender_data.get('pncp_id'):
                existing_tender = Tender.query.filter_by(pncp_id=tender_data['pncp_id']).first()
            
            if existing_tender:
                logger.debug(f"Tender already exists: {tender_data.get('pncp_id', 'No ID')}")
                return existing_tender
            
            # Create new tender
            tender = Tender(
                pncp_id=tender_data.get('pncp_id'),
                title=tender_data.get('title', '')[:500],
                description=tender_data.get('description', ''),
                organization_name=tender_data.get('organization_name', '')[:200],
                organization_cnpj=tender_data.get('organization_cnpj'),
                municipality_name=tender_data.get('municipality_name', '')[:100],
                municipality_ibge=tender_data.get('municipality_ibge'),
                state_code=tender_data.get('state_code'),
                publication_date=tender_data.get('publication_date'),
                update_date=tender_data.get('update_date'),
                status=tender_data.get('status', '')[:50],
                modality=tender_data.get('modality', '')[:100],
                estimated_value=tender_data.get('estimated_value'),
                source_url=tender_data.get('source_url'),
                downloaded_files=tender_data.get('downloaded_files', []),  # ← AQUI
                data_source=tender_data.get('data_source', 'UNKNOWN')
            )

            db.session.add(tender)
            db.session.commit()
            
            logger.debug(f"Saved tender: {tender.title}")
            return tender
            
        except IntegrityError as e:
            logger.warning(f"Duplicate tender detected: {e}")
            db.session.rollback()
            return None
        except Exception as e:
            logger.error(f"Error saving tender to database: {e}")
            logger.error(f"Tender data: {tender_data}")
            db.session.rollback()
            return None
    
    def scrape_pncp_data(self) -> int:
        """
        Scrape tender data from PNCP API
        
        Returns:
            Number of tenders saved
        """
        logger.info("Starting PNCP data scraping")
        saved_count = 0
        
        try:
            # Get IBGE codes for target cities
            ibge_codes = [city['ibge_code'] for city in self.target_cities]
            
            # Fetch tenders from PNCP
            tenders_data = self.pncp_client.fetch_tenders_for_cities(ibge_codes)
            
            # Save tenders to database
            for tender_data in tenders_data:
                tender = self.save_tender_to_db(tender_data)
                if tender:
                    saved_count += 1
            
            logger.info(f"PNCP scraping completed. Saved {saved_count} tenders")
            return saved_count
            
        except Exception as e:
            logger.error(f"Error during PNCP scraping: {e}")
            return saved_count
    
    def scrape_querido_diario_data(self) -> int:
        """
        Scrape tender data from Querido Diário API
        
        Returns:
            Number of tenders saved
        """
        logger.info("Starting Querido Diário data scraping")
        saved_count = 0
        
        try:
            # Get IBGE codes for target cities
            ibge_codes = [city['ibge_code'] for city in self.target_cities]
            
            # Fetch tenders from Querido Diário
            tenders_data = self.qd_client.fetch_tenders_for_cities(ibge_codes)
            
            # Save tenders to database
            for tender_data in tenders_data:
                tender = self.save_tender_to_db(tender_data)
                if tender:
                    saved_count += 1
            
            logger.info(f"Querido Diário scraping completed. Saved {saved_count} tenders")
            return saved_count
            
        except Exception as e:
            logger.error(f"Error during Querido Diário scraping: {e}")
            return saved_count
    
    def run_full_scraping(self) -> Dict[str, int]:
        """
        Run complete data scraping from both sources
        
        Returns:
            Dict with scraping results
        """
        logger.info("Starting full data scraping")
        
        results = {
            'pncp_count': 0,
            'querido_diario_count': 0,
            'total_count': 0,
            'errors': []
        }
        
        try:
            # Initialize cities
            self.initialize_cities()
            
            # Scrape PNCP data
            try:
                results['pncp_count'] = self.scrape_pncp_data()
            except Exception as e:
                error_msg = f"PNCP scraping failed: {e}"
                logger.error(error_msg)
                results['errors'].append(error_msg)
            
            # Scrape Querido Diário data
            try:
                results['querido_diario_count'] = self.scrape_querido_diario_data()
            except Exception as e:
                error_msg = f"Querido Diário scraping failed: {e}"
                logger.error(error_msg)
                results['errors'].append(error_msg)
            
            results['total_count'] = results['pncp_count'] + results['querido_diario_count']
            
            logger.info(f"Full scraping completed. Total: {results['total_count']} tenders")
            return results
            
        except Exception as e:
            error_msg = f"Full scraping failed: {e}"
            logger.error(error_msg)
            results['errors'].append(error_msg)
            return results
    
    def get_scraping_stats(self) -> Dict:
        """
        Get statistics about scraped data
        
        Returns:
            Dict with statistics
        """
        try:
            total_tenders = Tender.query.count()
            pncp_tenders = Tender.query.filter_by(data_source='PNCP').count()
            qd_tenders = Tender.query.filter_by(data_source='QUERIDO_DIARIO').count()
            
            # Get tenders by city
            city_stats = []
            for city in self.target_cities:
                city_count = Tender.query.filter_by(municipality_ibge=city['ibge_code']).count()
                city_stats.append({
                    'city': city['name'],
                    'ibge_code': city['ibge_code'],
                    'tender_count': city_count
                })
            
            # Get recent tenders
            recent_tenders = Tender.query.filter(
                Tender.publication_date >= date.today().replace(day=1)
            ).count()
            
            return {
                'total_tenders': total_tenders,
                'pncp_tenders': pncp_tenders,
                'querido_diario_tenders': qd_tenders,
                'recent_tenders': recent_tenders,
                'city_stats': city_stats,
                'last_updated': datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error getting scraping stats: {e}")
            return {
                'error': str(e),
                'last_updated': datetime.utcnow().isoformat()
            }

