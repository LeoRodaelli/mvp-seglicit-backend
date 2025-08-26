import requests
import logging
from datetime import datetime, date
from typing import List, Dict, Optional
import re
import time

logger = logging.getLogger(__name__)

class QueridoDiarioClient:
    """Client for Querido Diário API"""
    
    BASE_URL = "https://queridodiario.ok.org.br/api"
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'MVP-Licitacoes-Bot/1.0',
            'Accept': 'application/json'
        })
    
    def search_gazettes(
        self,
        territory_ids: List[str],
        querystring: str = "licitação OR edital OR pregão",
        published_since: Optional[str] = None,
        published_until: Optional[str] = None,
        size: int = 50,
        offset: int = 0
    ) -> Dict:
        """
        Search for content in gazettes
        
        Args:
            territory_ids: List of IBGE territory IDs
            querystring: Search query string
            published_since: Start date in YYYY-MM-DD format
            published_until: End date in YYYY-MM-DD format
            size: Number of results to return
            offset: Number of results to skip
            
        Returns:
            Dict containing API response data
        """
        endpoint = f"{self.BASE_URL}/gazettes"
        
        params = {
            'territory_ids': territory_ids,
            'querystring': querystring,
            'size': size,
            'offset': offset,
            'sort_by': 'descending_date'
        }
        
        if published_since:
            params['published_since'] = published_since
        if published_until:
            params['published_until'] = published_until
        
        try:
            logger.info(f"Searching gazettes in Querido Diário: {params}")
            response = self.session.get(endpoint, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            logger.info(f"Successfully found {len(data.get('gazettes', []))} gazette results")
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error searching gazettes in Querido Diário: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            raise
    
    def get_cities(self, name_filter: Optional[str] = None) -> Dict:
        """
        Get available cities from Querido Diário
        
        Args:
            name_filter: Optional city name filter
            
        Returns:
            Dict containing cities data
        """
        endpoint = f"{self.BASE_URL}/cities"
        
        params = {}
        if name_filter:
            params['city_name'] = name_filter
        
        try:
            logger.info(f"Fetching cities from Querido Diário: {params}")
            response = self.session.get(endpoint, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            logger.info(f"Successfully fetched {len(data.get('cities', []))} cities")
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching cities from Querido Diário: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            raise
    
    def extract_tender_info_from_excerpt(self, excerpt: str) -> Dict:
        """
        Extract tender information from gazette excerpt using regex patterns
        
        Args:
            excerpt: Text excerpt from gazette
            
        Returns:
            Dict with extracted tender information
        """
        tender_info = {
            'title': None,
            'modality': None,
            'object': None,
            'value': None,
            'deadline': None
        }
        
        try:
            # Common patterns for tender information
            patterns = {
                'edital_number': r'(?:EDITAL|AVISO)\s+(?:DE\s+)?(?:LICITAÇÃO\s+)?N[°º]?\s*(\d+/\d+)',
                'pregao_number': r'PREGÃO\s+(?:ELETRÔNICO\s+)?N[°º]?\s*(\d+/\d+)',
                'modality': r'(?:MODALIDADE|TIPO):\s*([A-ZÁÊÇÕ\s]+)',
                'object': r'(?:OBJETO|FINALIDADE):\s*([^.]+)',
                'value': r'(?:VALOR|PREÇO)\s*(?:ESTIMADO|MÁXIMO|TOTAL)?:?\s*R\$\s*([\d.,]+)',
                'deadline': r'(?:PRAZO|DATA\s+LIMITE|ENTREGA):\s*(\d{1,2}/\d{1,2}/\d{4})'
            }
            
            # Extract title (edital or pregão number)
            for pattern_name in ['edital_number', 'pregao_number']:
                match = re.search(patterns[pattern_name], excerpt, re.IGNORECASE)
                if match:
                    tender_info['title'] = match.group(0)
                    break
            
            # Extract other information
            for field, pattern in patterns.items():
                if field in ['edital_number', 'pregao_number']:
                    continue
                    
                match = re.search(pattern, excerpt, re.IGNORECASE)
                if match:
                    tender_info[field] = match.group(1).strip()
            
            return tender_info
            
        except Exception as e:
            logger.error(f"Error extracting tender info from excerpt: {e}")
            return tender_info
    
    def parse_gazette_data(self, gazette_raw: Dict) -> List[Dict]:
        """
        Parse raw gazette data from Querido Diário API into tender format
        
        Args:
            gazette_raw: Raw gazette data from API
            
        Returns:
            List of parsed tender data
        """
        tenders = []
        
        try:
            # Extract basic gazette info
            territory_id = gazette_raw.get('territory_id', '')
            territory_name = gazette_raw.get('territory_name', '')
            state_code = gazette_raw.get('state_code', '')
            date_str = gazette_raw.get('date', '')
            
            # Parse date
            pub_date = None
            if date_str:
                try:
                    pub_date = datetime.strptime(date_str, '%Y-%m-%d').date()
                except:
                    pass
            
            # Process excerpts
            for excerpt_data in gazette_raw.get('excerpts', []):
                excerpt_text = excerpt_data.get('excerpt', '')
                
                # Extract tender information from excerpt
                tender_info = self.extract_tender_info_from_excerpt(excerpt_text)
                
                # Only create tender if we found some relevant information
                if tender_info['title'] or any(tender_info.values()):
                    parsed_tender = {
                        'pncp_id': None,  # Not available from Querido Diário
                        'title': tender_info['title'] or 'Licitação identificada em diário oficial',
                        'description': excerpt_text[:500] + '...' if len(excerpt_text) > 500 else excerpt_text,
                        'organization_name': territory_name,
                        'organization_cnpj': None,
                        'municipality_name': territory_name,
                        'municipality_ibge': territory_id,
                        'state_code': state_code,
                        'publication_date': pub_date,
                        'update_date': datetime.utcnow(),
                        'status': 'Publicado',
                        'modality': tender_info['modality'],
                        'estimated_value': None,  # Would need more complex parsing
                        'source_url': gazette_raw.get('url', ''),
                        'data_source': 'QUERIDO_DIARIO'
                    }
                    
                    tenders.append(parsed_tender)
            
            return tenders
            
        except Exception as e:
            logger.error(f"Error parsing gazette data: {e}")
            logger.error(f"Raw data: {gazette_raw}")
            return []
    
    def fetch_tenders_for_cities(
        self,
        city_ibge_codes: List[str],
        days_back: int = 30
    ) -> List[Dict]:
        """
        Fetch tender-related content from gazettes for multiple cities
        
        Args:
            city_ibge_codes: List of IBGE codes for cities
            days_back: Number of days to look back from today
            
        Returns:
            List of parsed tender data
        """
        end_date = date.today()
        start_date = date.today().replace(day=1)  # First day of current month
        
        all_tenders = []
        
        try:
            logger.info(f"Fetching gazette tenders for cities: {city_ibge_codes}")
            
            # Search for tender-related content
            response = self.search_gazettes(
                territory_ids=city_ibge_codes,
                querystring="licitação OR edital OR pregão OR concorrência",
                published_since=start_date.isoformat(),
                published_until=end_date.isoformat(),
                size=100
            )
            
            # Parse gazette data
            for gazette_raw in response.get('gazettes', []):
                try:
                    parsed_tenders = self.parse_gazette_data(gazette_raw)
                    all_tenders.extend(parsed_tenders)
                except Exception as e:
                    logger.error(f"Error parsing gazette: {e}")
                    continue
            
            # Rate limiting
            time.sleep(1)
            
        except Exception as e:
            logger.error(f"Error fetching gazette tenders: {e}")
        
        logger.info(f"Total gazette tenders fetched: {len(all_tenders)}")
        return all_tenders

