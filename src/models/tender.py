from src.models.user import db
from datetime import datetime
from decimal import Decimal

class City(db.Model):
    __tablename__ = 'cities'
    
    ibge_code = db.Column(db.String(10), primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    state_code = db.Column(db.String(2), nullable=False)
    state_name = db.Column(db.String(50), nullable=False)
    
    # Relationship with tenders
    tenders = db.relationship('Tender', backref='city', lazy=True)
    
    def to_dict(self, include_files=True):
        """Convert tender to dictionary with enhanced information"""
        base_dict = {
            'id': self.id,
            'pncp_id': self.pncp_id,
            'title': self.title,
            'description': self.description,
            'organization_name': self.organization_name,
            'organization_cnpj': self.organization_cnpj,
            'municipality_name': self.municipality_name,
            'municipality_ibge': self.municipality_ibge,
            'state_code': self.state_code,
            'publication_date': self.publication_date.isoformat() if self.publication_date else None,
            'update_date': self.update_date.isoformat() if self.update_date else None,
            'status': self.status,
            'modality': self.modality,
            'estimated_value': float(self.estimated_value) if self.estimated_value else None,
            'source_url': self.source_url,
            'detail_url': self.detail_url,
            'data_source': self.data_source,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            
            # Informações adicionais formatadas
            'formatted_value': self._format_currency(self.estimated_value) if self.estimated_value else 'Valor não informado',
            'formatted_date': self._format_date(self.publication_date) if self.publication_date else 'Data não informada',
            'short_description': self._get_short_description(),
            'organization_info': self._get_organization_info(),
            'location_info': f"{self.municipality_name} - {self.state_code}" if self.municipality_name and self.state_code else 'Localização não informada',
            
            # Informações sobre arquivos
            'has_files': bool(self.downloaded_files and len(self.downloaded_files) > 0),
            'files_count': len(self.downloaded_files) if self.downloaded_files else 0,
            'pdf_available': self._has_pdf_available(),
            'main_file_url': self._get_main_file_url(),
            'pncp_url': self.get_pncp_url(),
        }
        
        if include_files:
            base_dict['downloaded_files'] = self.downloaded_files or []
        
        return base_dict
    
    def _format_currency(self, value):
        """Format currency value to Brazilian Real"""
        if not value:
            return 'Valor não informado'
        try:
            return f"R$ {float(value):,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
        except:
            return 'Valor não informado'
    
    def _format_date(self, date_obj):
        """Format date to Brazilian format"""
        if not date_obj:
            return 'Data não informada'
        try:
            if isinstance(date_obj, str):
                from datetime import datetime
                date_obj = datetime.strptime(date_obj, '%Y-%m-%d').date()
            return date_obj.strftime('%d/%m/%Y')
        except:
            return 'Data não informada'
    
    def _get_short_description(self):
        """Get shortened description for card display"""
        if not self.description:
            return 'Descrição não disponível'
        
        # Limitar a 200 caracteres
        if len(self.description) <= 200:
            return self.description
        
        return self.description[:200] + '...'
    
    def _get_organization_info(self):
        """Get formatted organization information"""
        if self.organization_name:
            if self.organization_cnpj:
                return f"{self.organization_name} (CNPJ: {self.organization_cnpj})"
            return self.organization_name
        return 'Organização não informada'
    
    def _has_pdf_available(self):
        """Check if tender has PDF files available"""
        if not self.downloaded_files:
            return False
        
        for file_info in self.downloaded_files:
            # Verificar se tem arquivo local ou URL
            if file_info.get('local_path') or file_info.get('url'):
                return True
        
        return False
    
    def _get_main_file_url(self):
        """Get URL for the main file (first PDF)"""
        if not self.downloaded_files or len(self.downloaded_files) == 0:
            return self.detail_url or self.source_url  # Usar detail_url se disponível
        
        main_file = self.downloaded_files[0]
        
        # Se tem arquivo local, retornar endpoint da API
        if main_file.get('local_path'):
            return f"/api/tenders/{self.id}/pdf"
        
        # Se tem URL externa, retornar ela
        if main_file.get('url'):
            return main_file['url']
        
        # Fallback para detail_url ou URL original
        return self.detail_url or self.source_url
    
    def get_pncp_url(self):
        """Get specific PNCP URL for this tender"""
        return self.detail_url or self.source_url
class Tender(db.Model):
    __tablename__ = 'tenders'
    
    id = db.Column(db.Integer, primary_key=True)
    pncp_id = db.Column(db.String(100), unique=True, nullable=True)
    title = db.Column(db.String(500), nullable=False)
    description = db.Column(db.Text)
    organization_name = db.Column(db.String(200))
    organization_cnpj = db.Column(db.String(20))
    municipality_name = db.Column(db.String(100))
    municipality_ibge = db.Column(db.String(10), db.ForeignKey('cities.ibge_code'))
    state_code = db.Column(db.String(2))
    publication_date = db.Column(db.Date)
    update_date = db.Column(db.DateTime)
    status = db.Column(db.String(50))
    modality = db.Column(db.String(100))
    estimated_value = db.Column(db.Numeric(15, 2))
    source_url = db.Column(db.Text)
    detail_url = db.Column(db.Text)  # URL específica do edital no PNCP
    downloaded_files = db.Column(db.JSON)
    data_source = db.Column(db.String(20))  # 'PNCP' or 'QUERIDO_DIARIO'
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    def to_dict(self, include_files=True):
        """Convert tender to dictionary with enhanced information"""
        base_dict = {
            'id': self.id,
            'pncp_id': self.pncp_id,
            'title': self.title,
            'description': self.description,
            'organization_name': self.organization_name,
            'organization_cnpj': self.organization_cnpj,
            'municipality_name': self.municipality_name,
            'municipality_ibge': self.municipality_ibge,
            'state_code': self.state_code,
            'publication_date': self.publication_date.isoformat() if self.publication_date else None,
            'update_date': self.update_date.isoformat() if self.update_date else None,
            'status': self.status,
            'modality': self.modality,
            'estimated_value': float(self.estimated_value) if self.estimated_value else None,
            'source_url': self.source_url,
            'detail_url': self.detail_url,
            'data_source': self.data_source,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            
            # Informações adicionais formatadas
            'formatted_value': self._format_currency(self.estimated_value) if self.estimated_value else 'Valor não informado',
            'formatted_date': self._format_date(self.publication_date) if self.publication_date else 'Data não informada',
            'short_description': self._get_short_description(),
            'organization_info': self._get_organization_info(),
            'location_info': f"{self.municipality_name} - {self.state_code}" if self.municipality_name and self.state_code else 'Localização não informada',
            
            # Informações sobre arquivos
            'has_files': bool(self.downloaded_files and len(self.downloaded_files) > 0),
            'files_count': len(self.downloaded_files) if self.downloaded_files else 0,
            'pdf_available': self._has_pdf_available(),
            'main_file_url': self._get_main_file_url(),
            'pncp_url': self.get_pncp_url(),
        }
        
        if include_files:
            base_dict['downloaded_files'] = self.downloaded_files or []
        
        return base_dict
    
    def _format_currency(self, value):
        """Format currency value to Brazilian Real"""
        if not value:
            return 'Valor não informado'
        try:
            return f"R$ {float(value):,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
        except:
            return 'Valor não informado'
    
    def _format_date(self, date_obj):
        """Format date to Brazilian format"""
        if not date_obj:
            return 'Data não informada'
        try:
            if isinstance(date_obj, str):
                from datetime import datetime
                date_obj = datetime.strptime(date_obj, '%Y-%m-%d').date()
            return date_obj.strftime('%d/%m/%Y')
        except:
            return 'Data não informada'
    
    def _get_short_description(self):
        """Get shortened description for card display"""
        if not self.description:
            return 'Descrição não disponível'
        
        # Limitar a 200 caracteres
        if len(self.description) <= 200:
            return self.description
        
        return self.description[:200] + '...'
    
    def _get_organization_info(self):
        """Get formatted organization information"""
        if self.organization_name:
            if self.organization_cnpj:
                return f"{self.organization_name} (CNPJ: {self.organization_cnpj})"
            return self.organization_name
        return 'Organização não informada'
    
    def _has_pdf_available(self):
        """Check if tender has PDF files available"""
        if not self.downloaded_files:
            return False
        
        for file_info in self.downloaded_files:
            # Verificar se tem arquivo local ou URL
            if file_info.get('local_path') or file_info.get('url'):
                return True
        
        return False
    
    def _get_main_file_url(self):
        """Get URL for the main file (first PDF)"""
        if not self.downloaded_files or len(self.downloaded_files) == 0:
            return self.detail_url or self.source_url  # Usar detail_url se disponível
        
        main_file = self.downloaded_files[0]
        
        # Se tem arquivo local, retornar endpoint da API
        if main_file.get('local_path'):
            return f"/api/tenders/{self.id}/pdf"
        
        # Se tem URL externa, retornar ela
        if main_file.get('url'):
            return main_file['url']
        
        # Fallback para detail_url ou URL original
        return self.detail_url or self.source_url
    
    def get_pncp_url(self):
        """Get specific PNCP URL for this tender"""
        return self.detail_url or self.source_url