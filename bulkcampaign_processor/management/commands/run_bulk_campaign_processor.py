from django.core.management.base import BaseCommand
import time
import schedule
import logging
import signal
import sys
from django.utils import timezone

from bulkcampaign_processor.tasks.bulk_campaign_tasks import (
    process_bulk_campaigns,
    process_due_messages
)

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = 'Runs the bulk campaign processor service'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.running = True
        signal.signal(signal.SIGINT, self.handle_shutdown)
        signal.signal(signal.SIGTERM, self.handle_shutdown)

    def handle_shutdown(self, sig, frame):
        logger.info('Shutting down bulk campaign processor...')
        self.stdout.write(self.style.WARNING('Shutting down bulk campaign processor...'))
        self.running = False

    def handle(self, *args, **options):
        logger.info('Starting bulk campaign processor service')
        self.stdout.write(self.style.SUCCESS('Starting bulk campaign processor service'))

        # Set up scheduled tasks
        schedule.every(5).minutes.do(self._safe_execute, process_bulk_campaigns)
        schedule.every(1).minutes.do(self._safe_execute, process_due_messages)

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

    def _safe_execute(self, func):
        """Safely execute a function and handle any errors"""
        try:
            result = func()
            logger.debug(f'Task {func.__name__} completed with result: {result}')
            return result
        except Exception as e:
            logger.exception(f'Error executing task {func.__name__}: {str(e)}')
            self.stderr.write(self.style.ERROR(f'Error executing task {func.__name__}: {str(e)}'))
            return None 