from src.models.user import db
from datetime import datetime
from decimal import Decimal
import json

class Edital(db.Model):
    __tablename__ = 'editais'
    
    id = db.Column(db.Integer, primary_key=True)
    pncp_id = db.Column(db.String(100), unique=True, nullable=True)
    title = db.Column(db.String(500), nullable=False)
    description = db.Column(db.Text)
    object_description = db.Column(db.Text)  # Objeto detalhado
    
    # Organização
    organization_name = db.Column(db.String(200))
    organization_cnpj = db.Column(db.String(20))
    
    # Localização
    municipality_name = db.Column(db.String(100))
    state_code = db.Column(db.String(2))
    state_name = db.Column(db.String(50))
    
    # Datas
    publication_date = db.Column(db.Date)
    update_date = db.Column(db.DateTime)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    # Licitação
    status = db.Column(db.String(50))
    modality = db.Column(db.String(100))
    estimated_value = db.Column(db.Numeric(15, 2))
    
    # URLs e fonte
    source_url = db.Column(db.Text)
    edital_url = db.Column(db.Text)  # URL específica do edital
    data_source = db.Column(db.String(20), default='PNCP')
    
    # Dados do scraper
    has_access_button = db.Column(db.Boolean, default=False)
    has_items_tab = db.Column(db.Boolean, default=False)
    has_files_tab = db.Column(db.Boolean, default=False)
    
    # Relacionamentos
    items = db.relationship('EditalItem', backref='edital', lazy=True, cascade='all, delete-orphan')
    files = db.relationship('EditalFile', backref='edital', lazy=True, cascade='all, delete-orphan')
    
    def to_dict(self):
        return {
            'id': self.id,
            'pncp_id': self.pncp_id,
            'title': self.title,
            'description': self.description,
            'object_description': self.object_description,
            'organization_name': self.organization_name,
            'organization_cnpj': self.organization_cnpj,
            'municipality_name': self.municipality_name,
            'state_code': self.state_code,
            'state_name': self.state_name,
            'publication_date': self.publication_date.isoformat() if self.publication_date else None,
            'update_date': self.update_date.isoformat() if self.update_date else None,
            'status': self.status,
            'modality': self.modality,
            'estimated_value': float(self.estimated_value) if self.estimated_value else None,
            'source_url': self.source_url,
            'edital_url': self.edital_url,
            'data_source': self.data_source,
            'has_access_button': self.has_access_button,
            'has_items_tab': self.has_items_tab,
            'has_files_tab': self.has_files_tab,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'items': [item.to_dict() for item in self.items],
            'files': [file.to_dict() for file in self.files]
        }

class EditalItem(db.Model):
    __tablename__ = 'edital_items'
    
    id = db.Column(db.Integer, primary_key=True)
    edital_id = db.Column(db.Integer, db.ForeignKey('editais.id'), nullable=False)
    
    numero = db.Column(db.String(20))
    descricao = db.Column(db.Text)
    quantidade = db.Column(db.Integer)
    valor_unitario = db.Column(db.Numeric(15, 2))
    valor_total = db.Column(db.Numeric(15, 2))
    
    # Dados brutos para debug
    raw_data = db.Column(db.Text)  # JSON com dados originais
    extraction_method = db.Column(db.String(50))
    
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    def to_dict(self):
        return {
            'id': self.id,
            'numero': self.numero,
            'descricao': self.descricao,
            'quantidade': self.quantidade,
            'valor_unitario': float(self.valor_unitario) if self.valor_unitario else None,
            'valor_total': float(self.valor_total) if self.valor_total else None,
            'raw_data': json.loads(self.raw_data) if self.raw_data else None,
            'extraction_method': self.extraction_method,
            'created_at': self.created_at.isoformat() if self.created_at else None
        }

class EditalFile(db.Model):
    __tablename__ = 'edital_files'
    
    id = db.Column(db.Integer, primary_key=True)
    edital_id = db.Column(db.Integer, db.ForeignKey('editais.id'), nullable=False)
    
    filename = db.Column(db.String(255))
    original_url = db.Column(db.Text)
    local_path = db.Column(db.Text)  # Caminho local do arquivo baixado
    file_size = db.Column(db.Integer)
    file_type = db.Column(db.String(50))  # PDF, DOC, etc.
    
    # Análise semântica do PDF (será implementada na Fase 3)
    extracted_text = db.Column(db.Text)
    semantic_data = db.Column(db.Text)  # JSON com dados extraídos
    
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    def to_dict(self):
        return {
            'id': self.id,
            'filename': self.filename,
            'original_url': self.original_url,
            'local_path': self.local_path,
            'file_size': self.file_size,
            'file_type': self.file_type,
            'extracted_text': self.extracted_text,
            'semantic_data': json.loads(self.semantic_data) if self.semantic_data else None,
            'created_at': self.created_at.isoformat() if self.created_at else None
        }

