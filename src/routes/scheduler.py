from flask import Blueprint, request, jsonify, current_app
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

scheduler_bp = Blueprint('scheduler', __name__)

@scheduler_bp.route('/scheduler/status', methods=['GET'])
def get_scheduler_status():
    """
    GET /api/scheduler/status
    Retorna status do sistema de agendamento
    """
    try:
        scheduler_service = current_app.scheduler_service
        status = scheduler_service.get_scheduler_status()
        
        return jsonify({
            'status': status,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Erro ao obter status do scheduler: {str(e)}")
        return jsonify({
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@scheduler_bp.route('/scheduler/jobs', methods=['GET'])
def get_jobs():
    """
    GET /api/scheduler/jobs
    Lista todos os jobs agendados
    """
    try:
        scheduler_service = current_app.scheduler_service
        jobs = scheduler_service.get_jobs()
        
        return jsonify({
            'jobs': jobs,
            'total_jobs': len(jobs),
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Erro ao listar jobs: {str(e)}")
        return jsonify({
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@scheduler_bp.route('/scheduler/jobs', methods=['POST'])
def create_custom_job():
    """
    POST /api/scheduler/jobs
    Cria um job customizado de scraping
    
    Body:
    {
        "states": ["SP", "RJ"],
        "cron_expression": "0 8 * * 1-5",
        "job_id": "custom_job_1",
        "name": "Scraping SP/RJ dias úteis"
    }
    """
    try:
        data = request.get_json() or {}
        
        states = data.get('states', [])
        cron_expression = data.get('cron_expression', '')
        job_id = data.get('job_id')
        name = data.get('name', f"Scraping Customizado - {', '.join(states)}")
        
        if not states:
            return jsonify({
                'error': 'Lista de estados é obrigatória'
            }), 400
        
        if not cron_expression:
            return jsonify({
                'error': 'Expressão cron é obrigatória'
            }), 400
        
        # Validar estados
        valid_states = ['AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 
                       'MA', 'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 
                       'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO']
        
        invalid_states = [s for s in states if s not in valid_states]
        if invalid_states:
            return jsonify({
                'error': f'Estados inválidos: {", ".join(invalid_states)}'
            }), 400
        
        scheduler_service = current_app.scheduler_service
        created_job_id = scheduler_service.schedule_custom_scraping(
            states=states,
            cron_expression=cron_expression,
            job_id=job_id
        )
        
        return jsonify({
            'success': True,
            'message': 'Job customizado criado com sucesso',
            'job_id': created_job_id,
            'states': states,
            'cron_expression': cron_expression,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Erro ao criar job customizado: {str(e)}")
        return jsonify({
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@scheduler_bp.route('/scheduler/jobs/<job_id>', methods=['DELETE'])
def remove_job(job_id):
    """
    DELETE /api/scheduler/jobs/{job_id}
    Remove um job agendado
    """
    try:
        scheduler_service = current_app.scheduler_service
        success = scheduler_service.remove_job(job_id)
        
        if success:
            return jsonify({
                'success': True,
                'message': f'Job "{job_id}" removido com sucesso',
                'timestamp': datetime.now().isoformat()
            })
        else:
            return jsonify({
                'error': f'Job "{job_id}" não encontrado',
                'timestamp': datetime.now().isoformat()
            }), 404
        
    except Exception as e:
        logger.error(f"Erro ao remover job {job_id}: {str(e)}")
        return jsonify({
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@scheduler_bp.route('/scheduler/jobs/<job_id>/pause', methods=['POST'])
def pause_job(job_id):
    """
    POST /api/scheduler/jobs/{job_id}/pause
    Pausa um job agendado
    """
    try:
        scheduler_service = current_app.scheduler_service
        success = scheduler_service.pause_job(job_id)
        
        if success:
            return jsonify({
                'success': True,
                'message': f'Job "{job_id}" pausado com sucesso',
                'timestamp': datetime.now().isoformat()
            })
        else:
            return jsonify({
                'error': f'Job "{job_id}" não encontrado',
                'timestamp': datetime.now().isoformat()
            }), 404
        
    except Exception as e:
        logger.error(f"Erro ao pausar job {job_id}: {str(e)}")
        return jsonify({
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@scheduler_bp.route('/scheduler/jobs/<job_id>/resume', methods=['POST'])
def resume_job(job_id):
    """
    POST /api/scheduler/jobs/{job_id}/resume
    Resume um job pausado
    """
    try:
        scheduler_service = current_app.scheduler_service
        success = scheduler_service.resume_job(job_id)
        
        if success:
            return jsonify({
                'success': True,
                'message': f'Job "{job_id}" resumido com sucesso',
                'timestamp': datetime.now().isoformat()
            })
        else:
            return jsonify({
                'error': f'Job "{job_id}" não encontrado',
                'timestamp': datetime.now().isoformat()
            }), 404
        
    except Exception as e:
        logger.error(f"Erro ao resumir job {job_id}: {str(e)}")
        return jsonify({
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@scheduler_bp.route('/scheduler/jobs/<job_id>/run', methods=['POST'])
def run_job_now(job_id):
    """
    POST /api/scheduler/jobs/{job_id}/run
    Executa um job imediatamente
    """
    try:
        scheduler_service = current_app.scheduler_service
        success = scheduler_service.run_job_now(job_id)
        
        if success:
            return jsonify({
                'success': True,
                'message': f'Job "{job_id}" executado com sucesso',
                'timestamp': datetime.now().isoformat()
            })
        else:
            return jsonify({
                'error': f'Job "{job_id}" não encontrado',
                'timestamp': datetime.now().isoformat()
            }), 404
        
    except Exception as e:
        logger.error(f"Erro ao executar job {job_id}: {str(e)}")
        return jsonify({
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@scheduler_bp.route('/scheduler/start', methods=['POST'])
def start_scheduler():
    """
    POST /api/scheduler/start
    Inicia o sistema de agendamento
    """
    try:
        scheduler_service = current_app.scheduler_service
        scheduler_service.start()
        
        return jsonify({
            'success': True,
            'message': 'Scheduler iniciado com sucesso',
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Erro ao iniciar scheduler: {str(e)}")
        return jsonify({
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@scheduler_bp.route('/scheduler/stop', methods=['POST'])
def stop_scheduler():
    """
    POST /api/scheduler/stop
    Para o sistema de agendamento
    """
    try:
        scheduler_service = current_app.scheduler_service
        scheduler_service.shutdown()
        
        return jsonify({
            'success': True,
            'message': 'Scheduler parado com sucesso',
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Erro ao parar scheduler: {str(e)}")
        return jsonify({
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@scheduler_bp.route('/scheduler/presets', methods=['GET'])
def get_schedule_presets():
    """
    GET /api/scheduler/presets
    Retorna presets de agendamento comuns
    """
    try:
        presets = {
            'daily_morning': {
                'name': 'Diário - Manhã (6h)',
                'cron': '0 6 * * *',
                'description': 'Executa todos os dias às 6h da manhã'
            },
            'daily_evening': {
                'name': 'Diário - Noite (18h)',
                'cron': '0 18 * * *',
                'description': 'Executa todos os dias às 18h'
            },
            'weekdays_morning': {
                'name': 'Dias Úteis - Manhã (8h)',
                'cron': '0 8 * * 1-5',
                'description': 'Executa de segunda a sexta às 8h'
            },
            'weekdays_evening': {
                'name': 'Dias Úteis - Tarde (14h)',
                'cron': '0 14 * * 1-5',
                'description': 'Executa de segunda a sexta às 14h'
            },
            'twice_daily': {
                'name': 'Duas Vezes ao Dia (9h e 15h)',
                'cron': '0 9,15 * * *',
                'description': 'Executa às 9h e 15h todos os dias'
            },
            'every_4_hours': {
                'name': 'A Cada 4 Horas',
                'cron': '0 */4 * * *',
                'description': 'Executa a cada 4 horas'
            },
            'weekly_monday': {
                'name': 'Semanal - Segunda (10h)',
                'cron': '0 10 * * 1',
                'description': 'Executa toda segunda-feira às 10h'
            },
            'monthly_first': {
                'name': 'Mensal - Primeiro Dia (7h)',
                'cron': '0 7 1 * *',
                'description': 'Executa no primeiro dia de cada mês às 7h'
            }
        }
        
        states_groups = {
            'sudeste': ['SP', 'RJ', 'MG', 'ES'],
            'sul': ['RS', 'SC', 'PR'],
            'nordeste': ['BA', 'PE', 'CE', 'PB', 'RN', 'AL', 'SE', 'PI', 'MA'],
            'norte': ['AM', 'PA', 'AC', 'RO', 'RR', 'AP', 'TO'],
            'centro_oeste': ['GO', 'MT', 'MS', 'DF'],
            'principais': ['SP', 'RJ', 'MG', 'RS', 'PR', 'SC', 'BA', 'GO', 'PE', 'CE']
        }
        
        return jsonify({
            'schedule_presets': presets,
            'state_groups': states_groups,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Erro ao obter presets: {str(e)}")
        return jsonify({
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@scheduler_bp.route('/scheduler/quick-setup', methods=['POST'])
def quick_setup():
    """
    POST /api/scheduler/quick-setup
    Configuração rápida com presets
    
    Body:
    {
        "preset": "weekdays_morning",
        "state_group": "sudeste",
        "custom_states": ["SP", "RJ"],  // opcional, sobrescreve state_group
        "job_name": "Scraping Sudeste"  // opcional
    }
    """
    try:
        data = request.get_json() or {}
        
        preset = data.get('preset', '')
        state_group = data.get('state_group', '')
        custom_states = data.get('custom_states', [])
        job_name = data.get('job_name', '')
        
        # Obter presets
        presets = {
            'daily_morning': '0 6 * * *',
            'daily_evening': '0 18 * * *',
            'weekdays_morning': '0 8 * * 1-5',
            'weekdays_evening': '0 14 * * 1-5',
            'twice_daily': '0 9,15 * * *',
            'every_4_hours': '0 */4 * * *',
            'weekly_monday': '0 10 * * 1',
            'monthly_first': '0 7 1 * *'
        }
        
        state_groups = {
            'sudeste': ['SP', 'RJ', 'MG', 'ES'],
            'sul': ['RS', 'SC', 'PR'],
            'nordeste': ['BA', 'PE', 'CE', 'PB', 'RN', 'AL', 'SE', 'PI', 'MA'],
            'norte': ['AM', 'PA', 'AC', 'RO', 'RR', 'AP', 'TO'],
            'centro_oeste': ['GO', 'MT', 'MS', 'DF'],
            'principais': ['SP', 'RJ', 'MG', 'RS', 'PR', 'SC', 'BA', 'GO', 'PE', 'CE']
        }
        
        if not preset or preset not in presets:
            return jsonify({
                'error': 'Preset inválido ou não especificado'
            }), 400
        
        # Determinar estados
        if custom_states:
            states = custom_states
        elif state_group and state_group in state_groups:
            states = state_groups[state_group]
        else:
            return jsonify({
                'error': 'Grupo de estados ou estados customizados devem ser especificados'
            }), 400
        
        # Gerar nome do job se não especificado
        if not job_name:
            preset_names = {
                'daily_morning': 'Diário Manhã',
                'daily_evening': 'Diário Noite',
                'weekdays_morning': 'Dias Úteis Manhã',
                'weekdays_evening': 'Dias Úteis Tarde',
                'twice_daily': 'Duas Vezes ao Dia',
                'every_4_hours': 'A Cada 4h',
                'weekly_monday': 'Semanal Segunda',
                'monthly_first': 'Mensal'
            }
            job_name = f"{preset_names.get(preset, preset)} - {', '.join(states)}"
        
        # Criar job
        cron_expression = presets[preset]
        job_id = f"quick_{preset}_{state_group or 'custom'}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        scheduler_service = current_app.scheduler_service
        created_job_id = scheduler_service.schedule_custom_scraping(
            states=states,
            cron_expression=cron_expression,
            job_id=job_id
        )
        
        return jsonify({
            'success': True,
            'message': 'Job criado com configuração rápida',
            'job_id': created_job_id,
            'job_name': job_name,
            'preset': preset,
            'states': states,
            'cron_expression': cron_expression,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Erro na configuração rápida: {str(e)}")
        return jsonify({
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

