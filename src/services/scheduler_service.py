import logging
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.executors.pool import ThreadPoolExecutor
import atexit

from src.services.scraper_integration import ScraperIntegrationService
from src.services.pdf_integration import PDFIntegrationService
from src.models.user import db

logger = logging.getLogger(__name__)

class SchedulerService:
    """Serviço de agendamento para automação de tarefas"""
    
    def __init__(self, app=None):
        self.scheduler = None
        self.app = app
        self.scraper_service = ScraperIntegrationService()
        self.pdf_service = PDFIntegrationService()
        
        if app:
            self.init_app(app)
    
    def init_app(self, app):
        """Inicializa o scheduler com a aplicação Flask"""
        self.app = app
        
        # Configuração do scheduler
        jobstores = {
            'default': SQLAlchemyJobStore(url=app.config['SQLALCHEMY_DATABASE_URI'])
        }
        
        executors = {
            'default': ThreadPoolExecutor(20),
        }
        
        job_defaults = {
            'coalesce': False,
            'max_instances': 3
        }
        
        self.scheduler = BackgroundScheduler(
            jobstores=jobstores,
            executors=executors,
            job_defaults=job_defaults,
            timezone='America/Sao_Paulo'
        )
        
        # Registrar shutdown
        atexit.register(lambda: self.shutdown())
    
    def start(self):
        """Inicia o scheduler"""
        if self.scheduler and not self.scheduler.running:
            try:
                self.scheduler.start()
                logger.info("Scheduler iniciado com sucesso")
                
                # Agendar jobs padrão
                self._schedule_default_jobs()
                
            except Exception as e:
                logger.error(f"Erro ao iniciar scheduler: {str(e)}")
    
    def shutdown(self):
        """Para o scheduler"""
        if self.scheduler and self.scheduler.running:
            try:
                self.scheduler.shutdown()
                logger.info("Scheduler parado com sucesso")
            except Exception as e:
                logger.error(f"Erro ao parar scheduler: {str(e)}")
    
    def _schedule_default_jobs(self):
        """Agenda jobs padrão do sistema"""
        try:
            # Job 1: Scraping diário às 6h da manhã
            self.scheduler.add_job(
                func=run_daily_scraping_job,
                trigger=CronTrigger(hour=6, minute=0),
                id='daily_scraping',
                name='Scraping Diário de Licitações',
                replace_existing=True,
                misfire_grace_time=3600  # 1 hora de tolerância
            )
            
            # Job 2: Análise de PDFs pendentes a cada 2 horas
            self.scheduler.add_job(
                func=run_pdf_analysis_job,
                trigger=IntervalTrigger(hours=2),
                id='pdf_analysis',
                name='Análise de PDFs Pendentes',
                replace_existing=True,
                misfire_grace_time=1800  # 30 minutos de tolerância
            )
            
            # Job 3: Limpeza de logs antigos semanalmente
            self.scheduler.add_job(
                func=run_cleanup_job,
                trigger=CronTrigger(day_of_week=6, hour=2, minute=0),  # Domingo
                id='weekly_cleanup',
                name='Limpeza Semanal de Dados',
                replace_existing=True,
                misfire_grace_time=7200  # 2 horas de tolerância
            )
            
            logger.info("Jobs padrão agendados com sucesso")
            
        except Exception as e:
            logger.error(f"Erro ao agendar jobs padrão: {str(e)}")
    
    def schedule_daily_scraping(self):
        """Agenda scraping diário de licitações"""
        try:
            # Scraping diário às 6h da manhã
            self.scheduler.add_job(
                func=self._run_daily_scraping,
                trigger=CronTrigger(hour=6, minute=0),
                id='daily_scraping',
                name='Scraping Diário de Licitações',
                replace_existing=True,
                misfire_grace_time=3600  # 1 hora de tolerância
            )
            
            logger.info("Job de scraping diário agendado para 06:00")
            
        except Exception as e:
            logger.error(f"Erro ao agendar scraping diário: {str(e)}")
    
    def schedule_pdf_analysis(self):
        """Agenda análise de PDFs pendentes"""
        try:
            # Análise de PDFs a cada 2 horas
            self.scheduler.add_job(
                func=self._run_pdf_analysis,
                trigger=IntervalTrigger(hours=2),
                id='pdf_analysis',
                name='Análise de PDFs Pendentes',
                replace_existing=True,
                misfire_grace_time=1800  # 30 minutos de tolerância
            )
            
            logger.info("Job de análise de PDFs agendado a cada 2 horas")
            
        except Exception as e:
            logger.error(f"Erro ao agendar análise de PDFs: {str(e)}")
    
    def schedule_cleanup(self):
        """Agenda limpeza de dados antigos"""
        try:
            # Limpeza semanal aos domingos às 2h da manhã
            self.scheduler.add_job(
                func=self._run_cleanup,
                trigger=CronTrigger(day_of_week=6, hour=2, minute=0),  # Domingo
                id='weekly_cleanup',
                name='Limpeza Semanal de Dados',
                replace_existing=True,
                misfire_grace_time=7200  # 2 horas de tolerância
            )
            
            logger.info("Job de limpeza semanal agendado para domingos às 02:00")
            
        except Exception as e:
            logger.error(f"Erro ao agendar limpeza: {str(e)}")
    
    def schedule_custom_scraping(self, states: List[str], cron_expression: str, job_id: str = None) -> str:
        """
        Agenda scraping customizado para estados específicos
        
        Args:
            states: Lista de códigos de estado (ex: ['SP', 'RJ'])
            cron_expression: Expressão cron (ex: '0 8 * * 1-5' para dias úteis às 8h)
            job_id: ID personalizado do job
            
        Returns:
            ID do job criado
        """
        try:
            if not job_id:
                job_id = f"custom_scraping_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            # Validar expressão cron
            trigger = CronTrigger.from_crontab(cron_expression)
            
            self.scheduler.add_job(
                func=run_custom_scraping_job,
                trigger=trigger,
                args=[states],
                id=job_id,
                name=f'Scraping Customizado - {", ".join(states)}',
                replace_existing=True,
                misfire_grace_time=1800
            )
            
            logger.info(f"Job customizado '{job_id}' agendado para estados {states}")
            return job_id
            
        except Exception as e:
            logger.error(f"Erro ao agendar scraping customizado: {str(e)}")
            raise
    
    def _run_daily_scraping(self):
        """Executa scraping diário para todos os estados principais"""
        logger.info("Iniciando scraping diário automático")
        
        try:
            with self.app.app_context():
                # Estados principais para scraping diário
                main_states = ['SP', 'RJ', 'MG', 'RS', 'PR', 'SC', 'BA', 'GO', 'PE', 'CE']
                
                results = {}
                for state in main_states:
                    try:
                        logger.info(f"Executando scraping para {state}")
                        result = self.scraper_service.run_scraping(
                            uf_filter=state,
                            limit_per_state=10  # Limite para não sobrecarregar
                        )
                        results[state] = result
                        
                        # Aguardar entre estados para não sobrecarregar o servidor
                        import time
                        time.sleep(30)
                        
                    except Exception as e:
                        logger.error(f"Erro no scraping de {state}: {str(e)}")
                        results[state] = {'error': str(e)}
                
                # Log do resultado
                total_editais = sum(r.get('editais_salvos', 0) for r in results.values() if 'editais_salvos' in r)
                logger.info(f"Scraping diário concluído. Total de editais coletados: {total_editais}")
                
                # Executar análise de PDFs dos novos editais
                self._run_pdf_analysis()
                
        except Exception as e:
            logger.error(f"Erro no scraping diário: {str(e)}")
    
    def _run_custom_scraping(self, states: List[str]):
        """Executa scraping customizado para estados específicos"""
        logger.info(f"Iniciando scraping customizado para estados: {', '.join(states)}")
        
        try:
            with self.app.app_context():
                results = {}
                for state in states:
                    try:
                        logger.info(f"Executando scraping customizado para {state}")
                        result = self.scraper_service.run_scraping(
                            uf_filter=state,
                            limit_per_state=20  # Limite maior para scraping customizado
                        )
                        results[state] = result
                        
                        # Aguardar entre estados
                        import time
                        time.sleep(45)
                        
                    except Exception as e:
                        logger.error(f"Erro no scraping customizado de {state}: {str(e)}")
                        results[state] = {'error': str(e)}
                
                total_editais = sum(r.get('editais_salvos', 0) for r in results.values() if 'editais_salvos' in r)
                logger.info(f"Scraping customizado concluído. Total de editais: {total_editais}")
                
        except Exception as e:
            logger.error(f"Erro no scraping customizado: {str(e)}")
    
    def _run_pdf_analysis(self):
        """Executa análise de PDFs pendentes"""
        logger.info("Iniciando análise de PDFs pendentes")
        
        try:
            with self.app.app_context():
                result = self.pdf_service.analyze_all_pending_files()
                
                analyzed = result.get('analyzed', 0)
                errors = result.get('errors', 0)
                
                logger.info(f"Análise de PDFs concluída. Analisados: {analyzed}, Erros: {errors}")
                
        except Exception as e:
            logger.error(f"Erro na análise de PDFs: {str(e)}")
    
    def _run_cleanup(self):
        """Executa limpeza de dados antigos"""
        logger.info("Iniciando limpeza de dados antigos")
        
        try:
            with self.app.app_context():
                # Limpeza de logs antigos (mais de 30 dias)
                cutoff_date = datetime.now() - timedelta(days=30)
                
                # Aqui você pode implementar limpeza específica
                # Por exemplo, remover arquivos temporários, logs antigos, etc.
                
                logger.info("Limpeza de dados concluída")
                
        except Exception as e:
            logger.error(f"Erro na limpeza: {str(e)}")
    
    def get_jobs(self) -> List[Dict]:
        """Retorna lista de jobs agendados"""
        try:
            if not self.scheduler:
                return []
            
            jobs = []
            for job in self.scheduler.get_jobs():
                jobs.append({
                    'id': job.id,
                    'name': job.name,
                    'next_run': job.next_run_time.isoformat() if job.next_run_time else None,
                    'trigger': str(job.trigger),
                    'func': job.func.__name__ if hasattr(job.func, '__name__') else str(job.func)
                })
            
            return jobs
            
        except Exception as e:
            logger.error(f"Erro ao obter jobs: {str(e)}")
            return []
    
    def remove_job(self, job_id: str) -> bool:
        """Remove um job agendado"""
        try:
            if self.scheduler:
                self.scheduler.remove_job(job_id)
                logger.info(f"Job '{job_id}' removido com sucesso")
                return True
            return False
            
        except Exception as e:
            logger.error(f"Erro ao remover job '{job_id}': {str(e)}")
            return False
    
    def pause_job(self, job_id: str) -> bool:
        """Pausa um job agendado"""
        try:
            if self.scheduler:
                self.scheduler.pause_job(job_id)
                logger.info(f"Job '{job_id}' pausado")
                return True
            return False
            
        except Exception as e:
            logger.error(f"Erro ao pausar job '{job_id}': {str(e)}")
            return False
    
    def resume_job(self, job_id: str) -> bool:
        """Resume um job pausado"""
        try:
            if self.scheduler:
                self.scheduler.resume_job(job_id)
                logger.info(f"Job '{job_id}' resumido")
                return True
            return False
            
        except Exception as e:
            logger.error(f"Erro ao resumir job '{job_id}': {str(e)}")
            return False
    
    def run_job_now(self, job_id: str) -> bool:
        """Executa um job imediatamente"""
        try:
            if self.scheduler:
                job = self.scheduler.get_job(job_id)
                if job:
                    job.func(*job.args, **job.kwargs)
                    logger.info(f"Job '{job_id}' executado manualmente")
                    return True
            return False
            
        except Exception as e:
            logger.error(f"Erro ao executar job '{job_id}': {str(e)}")
            return False
    
    def get_scheduler_status(self) -> Dict:
        """Retorna status do scheduler"""
        try:
            if not self.scheduler:
                return {'status': 'not_initialized'}
            
            return {
                'status': 'running' if self.scheduler.running else 'stopped',
                'total_jobs': len(self.scheduler.get_jobs()),
                'timezone': str(self.scheduler.timezone),
                'state': self.scheduler.state
            }
            
        except Exception as e:
            logger.error(f"Erro ao obter status do scheduler: {str(e)}")
            return {'status': 'error', 'error': str(e)}



# Funções estáticas para jobs (evitar problemas de serialização)
def run_daily_scraping_job():
    """Função estática para scraping diário"""
    from flask import current_app
    
    logger.info("Iniciando scraping diário automático")
    
    try:
        # Estados principais para scraping diário
        main_states = ['SP', 'RJ', 'MG', 'RS', 'PR', 'SC', 'BA', 'GO', 'PE', 'CE']
        
        scraper_service = ScraperIntegrationService()
        results = {}
        
        for state in main_states:
            try:
                logger.info(f"Executando scraping para {state}")
                result = scraper_service.run_scraping(
                    uf_filter=state,
                    limit_per_state=10  # Limite para não sobrecarregar
                )
                results[state] = result
                
                # Aguardar entre estados para não sobrecarregar o servidor
                import time
                time.sleep(30)
                
            except Exception as e:
                logger.error(f"Erro no scraping de {state}: {str(e)}")
                results[state] = {'error': str(e)}
        
        # Log do resultado
        total_editais = sum(r.get('editais_salvos', 0) for r in results.values() if 'editais_salvos' in r)
        logger.info(f"Scraping diário concluído. Total de editais coletados: {total_editais}")
        
        # Executar análise de PDFs dos novos editais
        run_pdf_analysis_job()
        
    except Exception as e:
        logger.error(f"Erro no scraping diário: {str(e)}")

def run_pdf_analysis_job():
    """Função estática para análise de PDFs"""
    logger.info("Iniciando análise de PDFs pendentes")
    
    try:
        pdf_service = PDFIntegrationService()
        result = pdf_service.analyze_all_pending_files()
        
        analyzed = result.get('analyzed', 0)
        errors = result.get('errors', 0)
        
        logger.info(f"Análise de PDFs concluída. Analisados: {analyzed}, Erros: {errors}")
        
    except Exception as e:
        logger.error(f"Erro na análise de PDFs: {str(e)}")

def run_cleanup_job():
    """Função estática para limpeza de dados"""
    logger.info("Iniciando limpeza de dados antigos")
    
    try:
        # Limpeza de logs antigos (mais de 30 dias)
        cutoff_date = datetime.now() - timedelta(days=30)
        
        # Aqui você pode implementar limpeza específica
        # Por exemplo, remover arquivos temporários, logs antigos, etc.
        
        logger.info("Limpeza de dados concluída")
        
    except Exception as e:
        logger.error(f"Erro na limpeza: {str(e)}")

def run_custom_scraping_job(states):
    """Função estática para scraping customizado"""
    logger.info(f"Iniciando scraping customizado para estados: {', '.join(states)}")
    
    try:
        scraper_service = ScraperIntegrationService()
        results = {}
        
        for state in states:
            try:
                logger.info(f"Executando scraping customizado para {state}")
                result = scraper_service.run_scraping(
                    uf_filter=state,
                    limit_per_state=20  # Limite maior para scraping customizado
                )
                results[state] = result
                
                # Aguardar entre estados
                import time
                time.sleep(45)
                
            except Exception as e:
                logger.error(f"Erro no scraping customizado de {state}: {str(e)}")
                results[state] = {'error': str(e)}
        
        total_editais = sum(r.get('editais_salvos', 0) for r in results.values() if 'editais_salvos' in r)
        logger.info(f"Scraping customizado concluído. Total de editais: {total_editais}")
        
    except Exception as e:
        logger.error(f"Erro no scraping customizado: {str(e)}")

