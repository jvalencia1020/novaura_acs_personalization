# journey_processor/tasks/queue_tasks.py

import json
import logging
import boto3
from botocore.exceptions import ClientError
from django.conf import settings
from django.db import transaction
from django.utils import timezone

from journey_processor.services.journey_processor import JourneyProcessor

logger = logging.getLogger(__name__)


def process_sqs_messages(queue_url=None, max_messages=10, wait_time=20, visibility_timeout=300):
    """
    Process messages from the journey events SQS queue

    Args:
        queue_url: URL of the SQS queue (defaults to settings.JOURNEY_EVENTS_QUEUE_URL)
        max_messages: Maximum number of messages to retrieve per batch (1-10)
        wait_time: Long polling wait time in seconds
        visibility_timeout: Visibility timeout for messages in seconds

    Returns:
        int: Number of messages processed successfully
    """
    if not queue_url:
        queue_url = settings.JOURNEY_EVENTS_QUEUE_URL

    if not queue_url:
        logger.error("No queue URL provided or found in settings")
        return 0

    try:
        sqs = boto3.client('sqs')
        processor = JourneyProcessor()

        # Receive messages from the queue
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=max_messages,
            WaitTimeSeconds=wait_time,
            VisibilityTimeout=visibility_timeout,
            AttributeNames=['All'],
            MessageAttributeNames=['All']
        )

        messages = response.get('Messages', [])

        if not messages:
            logger.debug("No messages received from the queue")
            return 0

        logger.info(f"Received {len(messages)} messages from SQS queue")

        processed_count = 0
        failed_messages = []

        for message in messages:
            receipt_handle = message['ReceiptHandle']
            message_id = message.get('MessageId', 'unknown')

            try:
                # Parse message body
                body = json.loads(message['Body'])

                event_type = body.get('event_type')
                event_data = body.get('data', {})

                if not event_type:
                    logger.warning(f"Message {message_id} missing event_type: {body}")
                    failed_messages.append((message_id, receipt_handle, "Missing event_type"))
                    continue

                logger.info(f"Processing message {message_id}, event type: {event_type}")

                # Process the event within a transaction
                with transaction.atomic():
                    result = processor.process_event(event_type, event_data)

                # Event processed successfully, delete from queue
                sqs.delete_message(
                    QueueUrl=queue_url,
                    ReceiptHandle=receipt_handle
                )

                logger.info(f"Successfully processed message {message_id}, affected {result} participants")
                processed_count += 1

            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON in message {message_id}: {e}")
                failed_messages.append((message_id, receipt_handle, f"Invalid JSON: {e}"))

            except Exception as e:
                logger.exception(f"Error processing message {message_id}: {e}")
                failed_messages.append((message_id, receipt_handle, f"Processing error: {e}"))

        # Handle failed messages - either retry or move to dead letter queue
        if failed_messages:
            logger.warning(f"Failed to process {len(failed_messages)} messages")
            _handle_failed_messages(queue_url, failed_messages, sqs)

        return processed_count

    except ClientError as e:
        logger.exception(f"AWS SQS client error: {e}")
        return 0

    except Exception as e:
        logger.exception(f"Unexpected error processing SQS messages: {e}")
        return 0


def publish_journey_event(event_type, data, queue_url=None, delay_seconds=0):
    """
    Publish a journey event to the SQS queue

    Args:
        event_type: Type of event (e.g., 'funnel_step_changed', 'email_opened')
        data: Dictionary with event data
        queue_url: SQS queue URL (defaults to settings.JOURNEY_EVENTS_QUEUE_URL)
        delay_seconds: Delay delivery of the message by this many seconds (0-900)

    Returns:
        dict: SQS response or None if error
    """
    if not queue_url:
        queue_url = settings.JOURNEY_EVENTS_QUEUE_URL

    if not queue_url:
        logger.error("No queue URL provided or found in settings")
        return None

    try:
        sqs = boto3.client('sqs')

        # Prepare message attributes for potential filtering
        message_attributes = {
            'EventType': {
                'DataType': 'String',
                'StringValue': event_type
            }
        }

        # Add lead_id as attribute if available for filtering
        if 'lead_id' in data:
            message_attributes['LeadId'] = {
                'DataType': 'String',
                'StringValue': str(data['lead_id'])
            }

        # Add participant_id as attribute if available
        if 'participant_id' in data:
            message_attributes['ParticipantId'] = {
                'DataType': 'String',
                'StringValue': str(data['participant_id'])
            }

        # Add connection_id as attribute if available
        if 'connection_id' in data:
            message_attributes['ConnectionId'] = {
                'DataType': 'String',
                'StringValue': str(data['connection_id'])
            }

        # Prepare message body
        message_body = json.dumps({
            'event_type': event_type,
            'data': data,
            'timestamp': _get_iso_timestamp()
        })

        # Send message to SQS
        response = sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=message_body,
            DelaySeconds=delay_seconds,
            MessageAttributes=message_attributes
        )

        logger.debug(f"Published {event_type} event to SQS: {response['MessageId']}")
        return response

    except ClientError as e:
        logger.exception(f"AWS SQS client error publishing event: {e}")
        return None

    except Exception as e:
        logger.exception(f"Unexpected error publishing event: {e}")
        return None


def process_batch_journey_events(events, queue_url=None):
    """
    Publish multiple journey events to the SQS queue in a batch

    Args:
        events: List of dicts, each with 'event_type' and 'data' keys
        queue_url: SQS queue URL (defaults to settings.JOURNEY_EVENTS_QUEUE_URL)

    Returns:
        dict: SQS response or None if error
    """
    if not events:
        logger.warning("No events provided for batch processing")
        return None

    if not queue_url:
        queue_url = settings.JOURNEY_EVENTS_QUEUE_URL

    if not queue_url:
        logger.error("No queue URL provided or found in settings")
        return None

    try:
        sqs = boto3.client('sqs')

        # Prepare batch entries (max 10 per batch)
        entries = []
        for i, event in enumerate(events[:10]):  # SQS allows max 10 messages per batch
            event_type = event.get('event_type')
            data = event.get('data', {})

            if not event_type:
                logger.warning(f"Skipping event at index {i}: missing event_type")
                continue

            # Prepare message attributes
            message_attributes = {
                'EventType': {
                    'DataType': 'String',
                    'StringValue': event_type
                }
            }

            # Add lead_id as attribute if available
            if 'lead_id' in data:
                message_attributes['LeadId'] = {
                    'DataType': 'String',
                    'StringValue': str(data['lead_id'])
                }

            # Add participant_id as attribute if available
            if 'participant_id' in data:
                message_attributes['ParticipantId'] = {
                    'DataType': 'String',
                    'StringValue': str(data['participant_id'])
                }

            # Add connection_id as attribute if available
            if 'connection_id' in data:
                message_attributes['ConnectionId'] = {
                    'DataType': 'String',
                    'StringValue': str(data['connection_id'])
                }

            # Create entry
            entries.append({
                'Id': f'msg-{i}',  # Unique ID for this batch
                'MessageBody': json.dumps({
                    'event_type': event_type,
                    'data': data,
                    'timestamp': _get_iso_timestamp()
                }),
                'MessageAttributes': message_attributes
            })

        if not entries:
            logger.warning("No valid entries found for batch processing")
            return None

        # Send batch to SQS
        response = sqs.send_message_batch(
            QueueUrl=queue_url,
            Entries=entries
        )

        if 'Failed' in response:
            logger.warning(f"Failed to send {len(response['Failed'])} messages in batch")
            for failure in response['Failed']:
                logger.warning(f"Failed message {failure['Id']}: {failure['Message']}")

        return response

    except ClientError as e:
        logger.exception(f"AWS SQS client error in batch processing: {e}")
        return None

    except Exception as e:
        logger.exception(f"Unexpected error in batch processing: {e}")
        return None


def purge_journey_queue(queue_url=None):
    """
    Purge all messages from the journey events queue

    Args:
        queue_url: SQS queue URL (defaults to settings.JOURNEY_EVENTS_QUEUE_URL)

    Returns:
        bool: True if successful, False otherwise
    """
    if not queue_url:
        queue_url = settings.JOURNEY_EVENTS_QUEUE_URL

    if not queue_url:
        logger.error("No queue URL provided or found in settings")
        return False

    try:
        sqs = boto3.client('sqs')
        sqs.purge_queue(QueueUrl=queue_url)
        logger.info("Successfully purged journey events queue")
        return True

    except ClientError as e:
        logger.exception(f"AWS SQS client error purging queue: {e}")
        return False

    except Exception as e:
        logger.exception(f"Unexpected error purging queue: {e}")
        return False


def get_queue_statistics(queue_url=None):
    """
    Get statistics about the journey events queue

    Args:
        queue_url: SQS queue URL (defaults to settings.JOURNEY_EVENTS_QUEUE_URL)

    Returns:
        dict: Queue statistics or None if error
    """
    if not queue_url:
        queue_url = settings.JOURNEY_EVENTS_QUEUE_URL

    if not queue_url:
        logger.error("No queue URL provided or found in settings")
        return None

    try:
        sqs = boto3.client('sqs')

        # Get queue attributes
        response = sqs.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=[
                'ApproximateNumberOfMessages',
                'ApproximateNumberOfMessagesDelayed',
                'ApproximateNumberOfMessagesNotVisible',
                'CreatedTimestamp',
                'LastModifiedTimestamp',
                'QueueArn'
            ]
        )

        attributes = response.get('Attributes', {})

        # Convert timestamp strings to datetime objects
        if 'CreatedTimestamp' in attributes:
            attributes['CreatedTimestamp'] = int(attributes['CreatedTimestamp'])
        if 'LastModifiedTimestamp' in attributes:
            attributes['LastModifiedTimestamp'] = int(attributes['LastModifiedTimestamp'])

        return attributes

    except ClientError as e:
        logger.exception(f"AWS SQS client error getting queue statistics: {e}")
        return None

    except Exception as e:
        logger.exception(f"Unexpected error getting queue statistics: {e}")
        return None


def _handle_failed_messages(queue_url, failed_messages, sqs_client=None):
    """
    Handle messages that failed to process

    Args:
        queue_url: SQS queue URL
        failed_messages: List of tuples (message_id, receipt_handle, error_reason)
        sqs_client: Optional boto3 SQS client
    """
    if not failed_messages:
        return

    try:
        if not sqs_client:
            sqs_client = boto3.client('sqs')

        # Get queue attributes to check for dead letter queue
        response = sqs_client.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=['RedrivePolicy']
        )

        redrive_policy = response.get('Attributes', {}).get('RedrivePolicy')
        if redrive_policy:
            try:
                policy = json.loads(redrive_policy)
                dead_letter_queue = policy.get('deadLetterTargetArn')
                max_receives = policy.get('maxReceiveCount', 5)

                # Move failed messages to dead letter queue
                for message_id, receipt_handle, error_reason in failed_messages:
                    try:
                        # Get message attributes
                        message = sqs_client.receive_message(
                            QueueUrl=queue_url,
                            MaxNumberOfMessages=1,
                            ReceiptHandle=receipt_handle
                        ).get('Messages', [{}])[0]

                        # Send to dead letter queue
                        sqs_client.send_message(
                            QueueUrl=dead_letter_queue,
                            MessageBody=message['Body'],
                            MessageAttributes=message.get('MessageAttributes', {}),
                            MessageDeduplicationId=message_id,
                            MessageGroupId='failed-messages'
                        )

                        # Delete from original queue
                        sqs_client.delete_message(
                            QueueUrl=queue_url,
                            ReceiptHandle=receipt_handle
                        )

                        logger.info(f"Moved failed message {message_id} to dead letter queue")

                    except Exception as e:
                        logger.exception(f"Error moving message {message_id} to dead letter queue: {e}")

            except json.JSONDecodeError:
                logger.error("Invalid RedrivePolicy JSON")

        else:
            # No dead letter queue configured, just log the failures
            for message_id, receipt_handle, error_reason in failed_messages:
                logger.error(f"Failed to process message {message_id}: {error_reason}")

    except Exception as e:
        logger.exception(f"Error handling failed messages: {e}")


def _get_iso_timestamp():
    """Get current timestamp in ISO format"""
    return timezone.now().isoformat()