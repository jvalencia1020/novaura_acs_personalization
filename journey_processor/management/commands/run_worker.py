# journey_processor/management/commands/run_worker.py
from django.core.management.base import BaseCommand
from django.conf import settings
import time
import signal
import sys
import boto3
import json
import logging
from django.utils import timezone
from botocore.exceptions import ClientError

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
        logger.info('Shutting down SQS worker...')
        self.stdout.write(self.style.WARNING('Shutting down SQS worker...'))
        self.running = False

    def handle(self, *args, **options):
        logger.info('Starting SQS worker for journey processing')
        self.stdout.write(self.style.SUCCESS('Starting SQS worker for journey processing'))

        queue_url = settings.JOURNEY_EVENTS_QUEUE_URL

        if not queue_url:
            error_msg = 'JOURNEY_EVENTS_QUEUE_URL not set'
            logger.error(error_msg)
            self.stderr.write(self.style.ERROR(error_msg))
            sys.exit(1)

        logger.info(f'Polling queue: {queue_url}')
        self.stdout.write(self.style.SUCCESS(f'Polling queue: {queue_url}'))

        # Initialize SQS client with retry configuration
        sqs = boto3.client('sqs', config=boto3.Config(
            retries=dict(
                max_attempts=3,
                mode='adaptive'
            )
        ))

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
                    logger.info(f"Processing {len(messages)} messages")
                    self.stdout.write(f"Processing {len(messages)} messages")

                    for message in messages:
                        try:
                            start_time = timezone.now()
                            
                            # Process message
                            body = json.loads(message['Body'])
                            event_type = body.get('event_type')
                            event_data = body.get('data', {})

                            if not event_type:
                                logger.warning(f"Message missing event_type: {body}")
                                continue

                            logger.info(f"Processing event: {event_type}")
                            self.processor.process_event(event_type, event_data)

                            # Delete message after successful processing
                            sqs.delete_message(
                                QueueUrl=queue_url,
                                ReceiptHandle=message['ReceiptHandle']
                            )

                            duration = timezone.now() - start_time
                            logger.info(f"Processed event {event_type} in {duration.total_seconds():.2f}s")

                        except json.JSONDecodeError as e:
                            logger.error(f"Invalid JSON in message: {e}")
                            self._handle_failed_message(sqs, queue_url, message, "Invalid JSON format")
                        except Exception as e:
                            logger.error(f"Error processing message: {e}", exc_info=True)
                            self._handle_failed_message(sqs, queue_url, message, str(e))

            except ClientError as e:
                error_code = e.response['Error']['Code']
                error_message = e.response['Error']['Message']
                logger.error(f"AWS SQS error ({error_code}): {error_message}")
                self.stderr.write(self.style.ERROR(f"AWS SQS error ({error_code}): {error_message}"))
                time.sleep(5)  # Back off on AWS errors
            except Exception as e:
                logger.error(f"Error receiving messages: {e}", exc_info=True)
                self.stderr.write(self.style.ERROR(f"Error receiving messages: {e}"))
                time.sleep(5)  # Back off on other errors

    def _handle_failed_message(self, sqs, queue_url, message, error_reason):
        """Handle a failed message by moving it to DLQ if configured"""
        try:
            # If DLQ is configured, move message there
            if hasattr(settings, 'JOURNEY_EVENTS_DLQ_URL'):
                sqs.send_message(
                    QueueUrl=settings.JOURNEY_EVENTS_DLQ_URL,
                    MessageBody=message['Body'],
                    MessageAttributes={
                        'ErrorReason': {
                            'DataType': 'String',
                            'StringValue': error_reason
                        },
                        'OriginalMessageId': {
                            'DataType': 'String',
                            'StringValue': message['MessageId']
                        }
                    }
                )
                logger.info(f"Moved failed message to DLQ: {message['MessageId']}")

            # Delete from main queue
            sqs.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=message['ReceiptHandle']
            )
        except Exception as e:
            logger.error(f"Error handling failed message: {e}", exc_info=True)