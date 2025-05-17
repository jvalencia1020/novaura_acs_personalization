# journey_processor/management/commands/run_scheduler.py
from django.core.management.base import BaseCommand
import time
import schedule
import logging
import signal
import sys
from django.utils import timezone

from journey_processor.tasks.scheduled_tasks import (
    process_timed_connections
)

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Runs the journey scheduler service'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.running = True
        signal.signal(signal.SIGINT, self.handle_shutdown)
        signal.signal(signal.SIGTERM, self.handle_shutdown)

    def handle_shutdown(self, sig, frame):
        logger.info('Shutting down scheduler...')
        self.stdout.write(self.style.WARNING('Shutting down scheduler...'))
        self.running = False

    def handle(self, *args, **options):
        logger.info('Starting journey scheduler service')
        self.stdout.write(self.style.SUCCESS('Starting journey scheduler service'))

        # Set up scheduled tasks
        schedule.every(5).minutes.do(self._safe_execute, process_timed_connections)

        self.stdout.write(self.style.SUCCESS('Scheduler initialized with tasks'))
        logger.info('Scheduler initialized with tasks')

        # Run scheduler loop
        while self.running:
            try:
                schedule.run_pending()
                time.sleep(1)
            except Exception as e:
                logger.error(f'Error in scheduler loop: {str(e)}', exc_info=True)
                self.stderr.write(self.style.ERROR(f'Error in scheduler loop: {str(e)}'))
                time.sleep(5)  # Back off on errors

    def _safe_execute(self, task_func):
        """Safely execute a scheduled task with error handling"""
        try:
            start_time = timezone.now()
            logger.info(f'Starting scheduled task: {task_func.__name__}')
            
            result = task_func()
            
            duration = timezone.now() - start_time
            logger.info(f'Completed scheduled task: {task_func.__name__} in {duration.total_seconds():.2f}s')
            return result
        except Exception as e:
            logger.error(f'Error executing scheduled task {task_func.__name__}: {str(e)}', exc_info=True)
            self.stderr.write(self.style.ERROR(f'Error executing scheduled task {task_func.__name__}: {str(e)}'))
            return None