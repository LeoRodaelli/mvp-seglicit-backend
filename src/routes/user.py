# -*- coding: utf-8 -*-
"""
API completa para autenticação de usuários
"""

from flask import Blueprint, request, jsonify
import psycopg2
import psycopg2.extras
import hashlib
import re
import logging
import json
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
import secrets
import string

load_dotenv()

# Configure logging
logger = logging.getLogger(__name__)

user_bp = Blueprint('user', __name__)

def get_db_connection():
    """Cria conexão direta com PostgreSQL"""
    try:
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT', 5432),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            client_encoding='utf8'
        )
        return conn
    except Exception as e:
        logger.error(f"Erro de conexão: {e}")
        return None

def validate_email(email):
    """Valida formato de email"""
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return re.match(pattern, email) is not None

def validate_cnpj_cpf(document):
    """Valida CNPJ ou CPF básico"""
    if not document:
        return True  # Campo opcional

    # Remove caracteres especiais
    clean_doc = re.sub(r'[^0-9]', '', document)

    # CPF: 11 dígitos
    if len(clean_doc) == 11:
        return True

    # CNPJ: 14 dígitos
    if len(clean_doc) == 14:
        return True

    return False

def hash_password(password):
    """Cria hash da senha"""
    return hashlib.sha256(password.encode()).hexdigest()

def generate_reset_token():
    """Gera token para reset de senha"""
    return ''.join(secrets.choice(string.ascii_letters + string.digits) for _ in range(32))

@user_bp.route('/register', methods=['POST'])
def register_user():
    """Registra novo usuário"""
    try:
        data = request.get_json()

        # Validações obrigatórias
        required_fields = ['username', 'email', 'password', 'full_name']
        for field in required_fields:
            if not data.get(field):
                return jsonify({
                    'success': False,
                    'error': f'Campo obrigatório: {field}'
                }), 400

        # Validar email
        if not validate_email(data['email']):
            return jsonify({
                'success': False,
                'error': 'Email inválido'
            }), 400

        # Validar senha
        if len(data['password']) < 6:
            return jsonify({
                'success': False,
                'error': 'Senha deve ter pelo menos 6 caracteres'
            }), 400

        # Validar CNPJ/CPF se fornecido
        if data.get('cnpj_cpf') and not validate_cnpj_cpf(data['cnpj_cpf']):
            return jsonify({
                'success': False,
                'error': 'CNPJ/CPF inválido'
            }), 400

        conn = get_db_connection()
        if not conn:
            raise Exception("Erro de conexão com banco")

        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # Verificar se username já existe
        cursor.execute("SELECT id FROM users WHERE username = %s", (data['username'],))
        if cursor.fetchone():
            return jsonify({
                'success': False,
                'error': 'Nome de usuário já existe'
            }), 400

        # Verificar se email já existe
        cursor.execute("SELECT id FROM users WHERE email = %s", (data['email'],))
        if cursor.fetchone():
            return jsonify({
                'success': False,
                'error': 'Email já cadastrado'
            }), 400

        # Criar hash da senha
        password_hash = hash_password(data['password'])

        # Inserir usuário
        insert_query = """
            INSERT INTO users (
                username, email, password_hash, full_name, phone, company_name,
                cnpj_cpf, address, city, state, zip_code, user_type,
                is_active, email_verified, created_at, updated_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            ) RETURNING id
        """

        cursor.execute(insert_query, (
            data['username'],
            data['email'],
            password_hash,
            data['full_name'],
            data.get('phone', ''),
            data.get('company_name', ''),
            data.get('cnpj_cpf', ''),
            data.get('address', ''),
            data.get('city', ''),
            data.get('state', ''),
            data.get('zip_code', ''),
            data.get('user_type', 'individual'),
            True,  # is_active
            False,  # email_verified
            datetime.now(),
            datetime.now()
        ))

        user_id = cursor.fetchone()['id']

        conn.commit()
        cursor.close()
        conn.close()

        logger.info(f"Usuário cadastrado: {data['username']} (ID: {user_id})")

        return jsonify({
            'success': True,
            'message': 'Usuário cadastrado com sucesso!',
            'user_id': user_id
        })

    except Exception as e:
        logger.error(f"Erro no cadastro: {e}")
        return jsonify({
            'success': False,
            'error': 'Erro interno do servidor'
        }), 500

@user_bp.route('/login', methods=['POST'])
def login_user():
    """Login de usuário com recursos avançados"""
    try:
        data = request.get_json()

        if not data.get('username') or not data.get('password'):
            return jsonify({
                'success': False,
                'error': 'Nome de usuário/email e senha são obrigatórios'
            }), 400

        conn = get_db_connection()
        if not conn:
            raise Exception("Erro de conexão com banco")

        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # Buscar usuário por username ou email
        cursor.execute("""
            SELECT id, username, email, full_name, phone, company_name, user_type,
                   is_active, email_verified, password_hash, created_at, last_login
            FROM users 
            WHERE (username = %s OR email = %s) AND is_active = true
        """, (data['username'], data['username']))

        user = cursor.fetchone()

        if not user:
            logger.warning(f"Tentativa de login com usuário inexistente: {data['username']}")
            return jsonify({
                'success': False,
                'error': 'Usuário não encontrado'
            }), 401

        # Verificar senha
        password_hash = hash_password(data['password'])
        if password_hash != user['password_hash']:
            logger.warning(f"Tentativa de login com senha incorreta: {user['username']}")
            return jsonify({
                'success': False,
                'error': 'Senha incorreta'
            }), 401

        # Atualizar último login
        cursor.execute(
            "UPDATE users SET last_login = %s WHERE id = %s",
            (datetime.now(), user['id'])
        )
        conn.commit()

        cursor.close()
        conn.close()

        # Preparar dados do usuário para resposta (sem senha)
        user_data = {
            'id': user['id'],
            'username': user['username'],
            'email': user['email'],
            'full_name': user['full_name'],
            'phone': user['phone'],
            'company_name': user['company_name'],
            'user_type': user['user_type'],
            'email_verified': user['email_verified'],
            'created_at': user['created_at'].isoformat() if user['created_at'] else None,
            'last_login': datetime.now().isoformat()
        }

        logger.info(f"Login realizado com sucesso: {user['username']} (ID: {user['id']})")

        return jsonify({
            'success': True,
            'message': 'Login realizado com sucesso!',
            'user': user_data
        })

    except Exception as e:
        logger.error(f"Erro no login: {e}")
        return jsonify({
            'success': False,
            'error': 'Erro interno do servidor'
        }), 500

@user_bp.route('/profile/<int:user_id>', methods=['GET'])
def get_user_profile(user_id):
    """Busca perfil completo do usuário"""
    try:
        conn = get_db_connection()
        if not conn:
            raise Exception("Erro de conexão com banco")

        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cursor.execute("""
            SELECT id, username, email, full_name, phone, company_name,
                   cnpj_cpf, address, city, state, zip_code, user_type,
                   is_active, email_verified, created_at, last_login, updated_at
            FROM users 
            WHERE id = %s AND is_active = true
        """, (user_id,))

        user = cursor.fetchone()

        if not user:
            return jsonify({
                'success': False,
                'error': 'Usuário não encontrado'
            }), 404

        cursor.close()
        conn.close()

        # Converter para dict e formatar datas
        user_data = dict(user)
        if user_data['created_at']:
            user_data['created_at'] = user_data['created_at'].isoformat()
        if user_data['last_login']:
            user_data['last_login'] = user_data['last_login'].isoformat()
        if user_data['updated_at']:
            user_data['updated_at'] = user_data['updated_at'].isoformat()

        return jsonify({
            'success': True,
            'user': user_data
        })

    except Exception as e:
        logger.error(f"Erro ao buscar perfil: {e}")
        return jsonify({
            'success': False,
            'error': 'Erro interno do servidor'
        }), 500

@user_bp.route('/check-availability', methods=['POST'])
def check_availability():
    """Verifica disponibilidade de username/email"""
    try:
        data = request.get_json()

        conn = get_db_connection()
        if not conn:
            raise Exception("Erro de conexão com banco")

        cursor = conn.cursor()

        result = {'available': True, 'message': ''}

        if data.get('username'):
            cursor.execute("SELECT id FROM users WHERE username = %s", (data['username'],))
            if cursor.fetchone():
                result = {'available': False, 'message': 'Nome de usuário já existe'}

        if data.get('email') and result['available']:
            cursor.execute("SELECT id FROM users WHERE email = %s", (data['email'],))
            if cursor.fetchone():
                result = {'available': False, 'message': 'Email já cadastrado'}

        cursor.close()
        conn.close()

        return jsonify({
            'success': True,
            **result
        })

    except Exception as e:
        logger.error(f"Erro na verificação: {e}")
        return jsonify({
            'success': False,
            'error': 'Erro interno do servidor'
        }), 500

@user_bp.route('/logout', methods=['POST'])
def logout_user():
    """Logout do usuário (placeholder para futuras funcionalidades)"""
    try:
        # Por enquanto, apenas retorna sucesso
        # Em implementações futuras, pode invalidar tokens, etc.

        return jsonify({
            'success': True,
            'message': 'Logout realizado com sucesso!'
        })

    except Exception as e:
        logger.error(f"Erro no logout: {e}")
        return jsonify({
            'success': False,
            'error': 'Erro interno do servidor'
        }), 500

@user_bp.route('/forgot-password', methods=['POST'])
def forgot_password():
    """Solicita reset de senha (placeholder)"""
    try:
        data = request.get_json()

        if not data.get('email'):
            return jsonify({
                'success': False,
                'error': 'Email é obrigatório'
            }), 400

        conn = get_db_connection()
        if not conn:
            raise Exception("Erro de conexão com banco")

        cursor = conn.cursor()

        # Verificar se email existe
        cursor.execute("SELECT id FROM users WHERE email = %s AND is_active = true", (data['email'],))
        user = cursor.fetchone()

        if not user:
            # Por segurança, sempre retorna sucesso mesmo se email não existir
            return jsonify({
                'success': True,
                'message': 'Se o email existir, você receberá instruções para reset da senha'
            })

        # Gerar token de reset (implementação futura)
        reset_token = generate_reset_token()
        expires_at = datetime.now() + timedelta(hours=1)

        # Salvar token no banco (implementação futura)
        # cursor.execute(
        #     "INSERT INTO password_reset_tokens (user_id, token, expires_at) VALUES (%s, %s, %s)",
        #     (user[0], reset_token, expires_at)
        # )

        cursor.close()
        conn.close()

        # Aqui seria enviado o email com o token (implementação futura)
        logger.info(f"Reset de senha solicitado para: {data['email']}")

        return jsonify({
            'success': True,
            'message': 'Se o email existir, você receberá instruções para reset da senha'
        })

    except Exception as e:
        logger.error(f"Erro no reset de senha: {e}")
        return jsonify({
            'success': False,
            'error': 'Erro interno do servidor'
        }), 500

@user_bp.route('/user-stats', methods=['GET'])
def get_user_stats():
    """Estatísticas de usuários"""
    try:
        conn = get_db_connection()
        if not conn:
            raise Exception("Erro de conexão com banco")

        cursor = conn.cursor()

        # Total de usuários
        cursor.execute("SELECT COUNT(*) FROM users WHERE is_active = true")
        total_users = cursor.fetchone()[0]

        # Usuários por tipo
        cursor.execute("""
            SELECT user_type, COUNT(*) 
            FROM users 
            WHERE is_active = true 
            GROUP BY user_type
        """)
        users_by_type = dict(cursor.fetchall())

        # Usuários cadastrados hoje
        cursor.execute("""
            SELECT COUNT(*) 
            FROM users 
            WHERE DATE(created_at) = CURRENT_DATE AND is_active = true
        """)
        users_today = cursor.fetchone()[0]

        # Usuários cadastrados esta semana
        cursor.execute("""
            SELECT COUNT(*) 
            FROM users 
            WHERE created_at >= CURRENT_DATE - INTERVAL '7 days' AND is_active = true
        """)
        users_this_week = cursor.fetchone()[0]

        cursor.close()
        conn.close()

        return jsonify({
            'success': True,
            'stats': {
                'total_users': total_users,
                'users_by_type': users_by_type,
                'users_today': users_today,
                'users_this_week': users_this_week
            }
        })

    except Exception as e:
        logger.error(f"Erro ao buscar estatísticas: {e}")
        return jsonify({
            'success': False,
            'error': 'Erro interno do servidor'
        }), 500

@user_bp.route('/test', methods=['GET'])
def test_user_api():
    """Testa conexão da API de usuários"""
    try:
        conn = get_db_connection()
        if not conn:
            raise Exception("Erro de conexão com banco")

        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM users")
        count = cursor.fetchone()[0]

        cursor.close()
        conn.close()

        return jsonify({
            'success': True,
            'message': 'API de usuários funcionando!',
            'total_users': count,
            'endpoints': [
                'POST /api/register - Cadastro de usuário',
                'POST /api/login - Login de usuário',
                'GET /api/profile/<id> - Perfil do usuário',
                'POST /api/check-availability - Verificar disponibilidade',
                'POST /api/logout - Logout',
                'POST /api/forgot-password - Reset de senha',
                'GET /api/stats - Estatísticas de usuários'
            ]
        })

    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500
