# journey_processor/management/commands/run_worker.py
from django.core.management.base import BaseCommand
from django.conf import settings
import time
import signal
import sys
import boto3
import json
import logging

from journey_processor.services.journey_processor import JourneyProcessor

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Runs the SQS worker for journey processing'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.running = True
        self.processor = JourneyProcessor()
        signal.signal(signal.SIGINT, self.handle_shutdown)
        signal.signal(signal.SIGTERM, self.handle_shutdown)

    def handle_shutdown(self, sig, frame):
        self.stdout.write(self.style.WARNING('Shutting down SQS worker...'))
        self.running = False

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS('Starting SQS worker for journey processing'))

        sqs = boto3.client('sqs')
        queue_url = settings.JOURNEY_EVENTS_QUEUE_URL

        if not queue_url:
            self.stderr.write(self.style.ERROR('JOURNEY_EVENTS_QUEUE_URL not set'))
            sys.exit(1)

        self.stdout.write(self.style.SUCCESS(f'Polling queue: {queue_url}'))

        # SQS worker loop
        while self.running:
            try:
                # Receive messages
                response = sqs.receive_message(
                    QueueUrl=queue_url,
                    MaxNumberOfMessages=10,
                    WaitTimeSeconds=20,  # Long polling
                    AttributeNames=['All'],
                    MessageAttributeNames=['All']
                )

                messages = response.get('Messages', [])

                if messages:
                    self.stdout.write(f"Processing {len(messages)} messages")

                    for message in messages:
                        try:
                            # Process message
                            body = json.loads(message['Body'])
                            event_type = body.get('event_type')
                            event_data = body.get('data', {})

                            if event_type:
                                self.processor.process_event(event_type, event_data)

                            # Delete message after successful processing
                            sqs.delete_message(
                                QueueUrl=queue_url,
                                ReceiptHandle=message['ReceiptHandle']
                            )

                        except Exception as e:
                            self.stderr.write(self.style.ERROR(f"Error processing message: {e}"))

            except Exception as e:
                self.stderr.write(self.style.ERROR(f"Error receiving messages: {e}"))
                time.sleep(5)  # Back off on errors