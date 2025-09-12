import os
import sys
import logging

# FORÇAR ENCODING UTF-8 ANTES DE QUALQUER IMPORT
os.environ['PYTHONIOENCODING'] = 'utf-8'
os.environ['PYTHONUTF8'] = '1'

# DON'T CHANGE THIS !!!
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from flask import Flask, send_from_directory
from flask_cors import CORS
from src.models.user import db
from src.models.tender import Tender, City
from src.routes.user import user_bp
from src.routes.tender import tender_bp
import os
from dotenv import load_dotenv

from src.routes.user import user_bp

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

app = Flask(__name__, static_folder=os.path.join(os.path.dirname(__file__), 'static'))
app.config['SECRET_KEY'] = 'asdf#FGSgvasgf$5$WGT'

# Enable CORS for all routes
CORS(app)

# Register blueprints
app.register_blueprint(user_bp, url_prefix='/api')
app.register_blueprint(tender_bp, url_prefix='/api')

load_dotenv()

# Database configuration com ENCODING FORÇADO
if os.getenv('DB_HOST'):
    # PostgreSQL na nuvem com UTF-8 FORÇADO
    DATABASE_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}?client_encoding=utf8"
    app.config['SQLALCHEMY_DATABASE_URI'] = DATABASE_URL

    # CONFIGURAÇÕES CRÍTICAS DE ENCODING
    app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {
        'pool_pre_ping': True,
        'pool_recycle': 300,
        'connect_args': {
            'client_encoding': 'utf8',
            'application_name': 'mvp_licitacoes',
            'options': '-c client_encoding=utf8'
        },
        'echo': False  # Desabilitar logs SQL para evitar problemas de encoding
    }
else:
    # SQLite local (fallback)
    app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///database/app.db'

app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# Initialize database
db.init_app(app)

@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def serve(path):
    static_folder_path = app.static_folder
    if static_folder_path is None:
            return "Static folder not configured", 404

    if path != "" and os.path.exists(os.path.join(static_folder_path, path)):
        return send_from_directory(static_folder_path, path)
    else:
        index_path = os.path.join(static_folder_path, 'index.html')
        if os.path.exists(index_path):
            return send_from_directory(static_folder_path, 'index.html')
        else:
            return "index.html not found", 404

# Configuração para produção
PORT = int(os.getenv('PORT', 5000))
DEBUG = os.getenv('FLASK_ENV') != 'production'

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=PORT, debug=DEBUG)
