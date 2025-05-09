# journey_processor/management/commands/run_scheduler.py
from django.core.management.base import BaseCommand
import time
import schedule
import logging
import signal
import sys

from journey_processor.tasks.scheduled_tasks import (
    process_timed_connections,
    enroll_leads_in_journeys
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
        self.stdout.write(self.style.WARNING('Shutting down scheduler...'))
        self.running = False

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS('Starting journey scheduler service'))

        # Set up scheduled tasks
        schedule.every(5).minutes.do(process_timed_connections)
        schedule.every(1).hour.do(enroll_leads_in_journeys)

        self.stdout.write(self.style.SUCCESS('Scheduler initialized with tasks'))

        # Run scheduler loop
        while self.running:
            schedule.run_pending()
            time.sleep(1)