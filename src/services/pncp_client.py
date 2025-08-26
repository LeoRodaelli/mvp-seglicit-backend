import requests
import logging
from datetime import datetime, date
from typing import List, Dict, Optional
import time

logger = logging.getLogger(__name__)

class PNCPClient:
    """Client for PNCP (Portal Nacional de Contratações Públicas) API"""
    
    BASE_URL = "https://pncp.gov.br/api/consulta"
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'MVP-Licitacoes-Bot/1.0',
            'Accept': 'application/json'
        })
    
    def get_tenders_by_publication_date(
        self,
        start_date: str,
        end_date: str,
        municipality_ibge: Optional[str] = None,
        page: int = 1,
        page_size: int = 50
    ) -> Dict:
        """
        Fetch tenders by publication date from PNCP API
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            municipality_ibge: IBGE code for municipality filter
            page: Page number (1-based)
            page_size: Number of results per page
            
        Returns:
            Dict containing API response data
        """
        endpoint = f"{self.BASE_URL}/v1/contratacoes/publicacao"
        
        params = {
            'dataInicial': start_date,
            'dataFinal': end_date,
            'pagina': page,
            'tamanhoPagina': page_size
        }
        
        if municipality_ibge:
            params['codigoMunicipioIbge'] = municipality_ibge
        
        try:
            logger.info(f"Fetching tenders from PNCP: {params}")
            response = self.session.get(endpoint, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            logger.info(f"Successfully fetched {len(data.get('data', []))} tenders")
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching data from PNCP: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            raise
    
    def get_tenders_with_open_proposals(
        self,
        municipality_ibge: Optional[str] = None,
        page: int = 1,
        page_size: int = 50
    ) -> Dict:
        """
        Fetch tenders with open proposals from PNCP API
        
        Args:
            municipality_ibge: IBGE code for municipality filter
            page: Page number (1-based)
            page_size: Number of results per page
            
        Returns:
            Dict containing API response data
        """
        endpoint = f"{self.BASE_URL}/v1/contratacoes/proposta"
        
        params = {
            'pagina': page,
            'tamanhoPagina': page_size
        }
        
        if municipality_ibge:
            params['codigoMunicipioIbge'] = municipality_ibge
        
        try:
            logger.info(f"Fetching open tenders from PNCP: {params}")
            response = self.session.get(endpoint, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            logger.info(f"Successfully fetched {len(data.get('data', []))} open tenders")
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching open tenders from PNCP: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            raise
    
    def parse_tender_data(self, tender_raw: Dict) -> Dict:
        """
        Parse raw tender data from PNCP API into standardized format
        
        Args:
            tender_raw: Raw tender data from API
            
        Returns:
            Dict with parsed tender data
        """
        try:
            # Extract organization info
            org_info = tender_raw.get('orgaoEntidade', {})
            unit_info = tender_raw.get('unidadeOrgao', {})
            legal_info = tender_raw.get('amparoLegal', {})
            
            # Parse dates
            pub_date = None
            if tender_raw.get('dataPublicacaoPncp'):
                try:
                    pub_date = datetime.fromisoformat(
                        tender_raw['dataPublicacaoPncp'].replace('Z', '+00:00')
                    ).date()
                except:
                    pass
            
            update_date = None
            if tender_raw.get('dataAtualizacao'):
                try:
                    update_date = datetime.fromisoformat(
                        tender_raw['dataAtualizacao'].replace('Z', '+00:00')
                    )
                except:
                    pass
            
            # Build PNCP ID
            pncp_id = None
            if tender_raw.get('numeroControlePncp'):
                pncp_id = tender_raw['numeroControlePncp']
            elif all([tender_raw.get('anoCompra'), tender_raw.get('sequencialCompra'), org_info.get('cnpj')]):
                pncp_id = f"{org_info['cnpj']}-{tender_raw['anoCompra']}-{tender_raw['sequencialCompra']}"
            
            parsed_data = {
                'pncp_id': pncp_id,
                'title': tender_raw.get('numeroCompra', 'Sem título'),
                'description': legal_info.get('descricao', ''),
                'organization_name': org_info.get('razaoSocial', ''),
                'organization_cnpj': org_info.get('cnpj', ''),
                'municipality_name': unit_info.get('municipioNome', ''),
                'municipality_ibge': unit_info.get('codigoIbge', ''),
                'state_code': unit_info.get('ufSigla', ''),
                'publication_date': pub_date,
                'update_date': update_date,
                'status': 'Publicado',  # Default status
                'modality': legal_info.get('nome', ''),
                'estimated_value': None,  # Not available in this endpoint
                'source_url': f"https://pncp.gov.br/app/editais/{pncp_id}" if pncp_id else None,
                'data_source': 'PNCP'
            }
            
            return parsed_data
            
        except Exception as e:
            logger.error(f"Error parsing tender data: {e}")
            logger.error(f"Raw data: {tender_raw}")
            raise
    
    def fetch_tenders_for_cities(
        self,
        city_ibge_codes: List[str],
        days_back: int = 30
    ) -> List[Dict]:
        """
        Fetch tenders for multiple cities
        
        Args:
            city_ibge_codes: List of IBGE codes for cities
            days_back: Number of days to look back from today
            
        Returns:
            List of parsed tender data
        """
        end_date = date.today()
        start_date = date.today().replace(day=1)  # First day of current month
        
        all_tenders = []
        
        for ibge_code in city_ibge_codes:
            try:
                logger.info(f"Fetching tenders for city {ibge_code}")
                
                # Fetch recent tenders
                response = self.get_tenders_by_publication_date(
                    start_date=start_date.isoformat(),
                    end_date=end_date.isoformat(),
                    municipality_ibge=ibge_code,
                    page=1,
                    page_size=100
                )
                
                # Parse tender data
                for tender_raw in response.get('data', []):
                    try:
                        parsed_tender = self.parse_tender_data(tender_raw)
                        all_tenders.append(parsed_tender)
                    except Exception as e:
                        logger.error(f"Error parsing tender: {e}")
                        continue
                
                # Fetch open proposals
                try:
                    open_response = self.get_tenders_with_open_proposals(
                        municipality_ibge=ibge_code,
                        page=1,
                        page_size=50
                    )
                    
                    for tender_raw in open_response.get('data', []):
                        try:
                            parsed_tender = self.parse_tender_data(tender_raw)
                            parsed_tender['status'] = 'Recebendo Propostas'
                            all_tenders.append(parsed_tender)
                        except Exception as e:
                            logger.error(f"Error parsing open tender: {e}")
                            continue
                            
                except Exception as e:
                    logger.warning(f"Could not fetch open proposals for {ibge_code}: {e}")
                
                # Rate limiting
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"Error fetching tenders for city {ibge_code}: {e}")
                continue
        
        logger.info(f"Total tenders fetched: {len(all_tenders)}")
        return all_tenders

