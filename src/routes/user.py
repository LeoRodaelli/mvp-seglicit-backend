# -*- coding: utf-8 -*-
"""
API completa para autenticação de usuários
"""
import bcrypt
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
    """Login de usuário com bcrypt"""
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
            cursor.close()
            conn.close()
            return jsonify({
                'success': False,
                'error': 'Usuário não encontrado'
            }), 401

        # ✅ CORREÇÃO: Verificar senha com bcrypt
        password_hash_db = user['password_hash']
        password_input = data['password']

        # Verificar se o hash no banco é bcrypt ou SHA256
        if password_hash_db.startswith('$2b$') or password_hash_db.startswith('$2a$'):
            # Hash é bcrypt (correto)
            password_match = bcrypt.checkpw(
                password_input.encode('utf-8'),
                password_hash_db.encode('utf-8')
            )
        else:
            # Hash é SHA256 (antigo) - converter para bcrypt
            import hashlib
            sha256_hash = hashlib.sha256(password_input.encode()).hexdigest()
            password_match = (sha256_hash == password_hash_db)

            # Se senha correta, atualizar para bcrypt
            if password_match:
                new_hash = bcrypt.hashpw(
                    password_input.encode('utf-8'),
                    bcrypt.gensalt()
                ).decode('utf-8')

                cursor.execute(
                    "UPDATE users SET password_hash = %s WHERE id = %s",
                    (new_hash, user['id'])
                )
                logger.info(f"✅ Senha do usuário {user['username']} migrada para bcrypt")

        if not password_match:
            logger.warning(f"Tentativa de login com senha incorreta: {user['username']}")
            cursor.close()
            conn.close()
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
    """Solicita reset de senha — gera token e envia email"""
    try:
        data = request.get_json()

        if not data.get('email'):
            return jsonify({'success': False, 'error': 'Email é obrigatório'}), 400

        conn = get_db_connection()
        if not conn:
            raise Exception("Erro de conexão com banco")

        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # Garantir que a tabela de tokens existe
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS password_reset_tokens (
                id SERIAL PRIMARY KEY,
                user_id INTEGER NOT NULL,
                token VARCHAR(64) UNIQUE NOT NULL,
                expires_at TIMESTAMP NOT NULL,
                used BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        conn.commit()

        # Buscar usuário — por segurança sempre retorna a mesma mensagem
        cursor.execute(
            "SELECT id, full_name FROM users WHERE email = %s AND is_active = true",
            (data['email'],)
        )
        user = cursor.fetchone()

        if user:
            reset_token = generate_reset_token()
            expires_at = datetime.now() + timedelta(hours=1)

            # Invalidar tokens anteriores do usuário
            cursor.execute(
                "UPDATE password_reset_tokens SET used = TRUE WHERE user_id = %s AND used = FALSE",
                (user['id'],)
            )

            cursor.execute(
                "INSERT INTO password_reset_tokens (user_id, token, expires_at) VALUES (%s, %s, %s)",
                (user['id'], reset_token, expires_at)
            )
            conn.commit()

            try:
                import threading as _threading
                from src.services.email_service import send_password_reset
                _args = (data['email'], user['full_name'], reset_token)
                _t = _threading.Thread(target=lambda: send_password_reset(*_args), daemon=True)
                _t.start()
                logger.info(f"📧 Email de reset agendado em background para: {data['email']}")
            except Exception as email_err:
                logger.error(f"❌ Erro ao agendar email de reset: {email_err}")

            logger.info(f"Reset de senha solicitado para: {data['email']}")

        cursor.close()
        conn.close()

        return jsonify({
            'success': True,
            'message': 'Se o email existir, você receberá instruções para redefinir a senha'
        })

    except Exception as e:
        logger.error(f"Erro no reset de senha: {e}")
        return jsonify({'success': False, 'error': 'Erro interno do servidor'}), 500


@user_bp.route('/reset-password', methods=['POST'])
def reset_password():
    """Redefine a senha usando token válido"""
    try:
        data = request.get_json()

        if not data.get('token') or not data.get('password'):
            return jsonify({'success': False, 'error': 'Token e nova senha são obrigatórios'}), 400

        if len(data['password']) < 6:
            return jsonify({'success': False, 'error': 'Senha deve ter pelo menos 6 caracteres'}), 400

        conn = get_db_connection()
        if not conn:
            raise Exception("Erro de conexão com banco")

        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cursor.execute("""
            SELECT user_id, expires_at, used
            FROM password_reset_tokens
            WHERE token = %s
        """, (data['token'],))
        token_row = cursor.fetchone()

        if not token_row:
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'error': 'Token inválido'}), 400

        if token_row['used']:
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'error': 'Token já utilizado'}), 400

        if datetime.now() > token_row['expires_at']:
            cursor.close()
            conn.close()
            return jsonify({'success': False, 'error': 'Token expirado. Solicite um novo.'}), 400

        # Atualizar senha
        new_hash = bcrypt.hashpw(
            data['password'].encode('utf-8'),
            bcrypt.gensalt()
        ).decode('utf-8')

        cursor.execute(
            "UPDATE users SET password_hash = %s, updated_at = NOW() WHERE id = %s",
            (new_hash, token_row['user_id'])
        )

        # Marcar token como usado
        cursor.execute(
            "UPDATE password_reset_tokens SET used = TRUE WHERE token = %s",
            (data['token'],)
        )

        conn.commit()
        cursor.close()
        conn.close()

        logger.info(f"✅ Senha redefinida para user_id: {token_row['user_id']}")

        return jsonify({'success': True, 'message': 'Senha redefinida com sucesso!'})

    except Exception as e:
        logger.error(f"Erro ao redefinir senha: {e}")
        return jsonify({'success': False, 'error': 'Erro interno do servidor'}), 500

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

@user_bp.route('/user/validate-email', methods=['POST'])
def validate_email():
    """Valida formato e domínio do email para checkout/assinatura MP."""
    try:
        data = request.get_json() or {}
        email = data.get('email', '')
        from src.services.email_validation import validate_checkout_email
        result = validate_checkout_email(email)
        return jsonify({'success': True, **result}), 200
    except Exception as e:
        logger.error('Erro ao validar email: %s', e)
        return jsonify({'success': False, 'valid': False, 'message': 'Erro ao validar email'}), 500

@user_bp.route('/user/subscription', methods=['GET'])
def get_user_subscription():
    user_id = request.args.get('user_id', type=int)
    if not user_id:
        return jsonify({'success': False, 'error': 'user_id obrigatório'}), 400

    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cursor.execute("""
        SELECT id, plan_id, plan_name, selected_states, selected_areas, status,
               current_period_end, billing_type, created_at
        FROM subscriptions
        WHERE user_id = %s AND status = 'active'
          AND (current_period_end IS NULL OR current_period_end >= CURRENT_DATE)
        ORDER BY created_at DESC
        LIMIT 1
    """, (user_id,))
    row = cursor.fetchone()
    cursor.close()
    conn.close()

    if not row:
        return jsonify({'success': True, 'subscription': None})

    sub = dict(row)
    # Parsear JSON strings
    import json
    for field in ['selected_states', 'selected_areas']:
        if isinstance(sub[field], str):
            try:
                sub[field] = json.loads(sub[field])
            except:
                sub[field] = []
    if sub['created_at']:
        sub['created_at'] = sub['created_at'].isoformat()
    if sub.get('current_period_end'):
        sub['current_period_end'] = sub['current_period_end'].isoformat()

    return jsonify({'success': True, 'subscription': sub})

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
