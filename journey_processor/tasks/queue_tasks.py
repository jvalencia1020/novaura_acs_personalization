# journey_processor/tasks/queue_tasks.py

import json
import logging
import boto3
from botocore.exceptions import ClientError
from django.conf import settings
from django.db import transaction

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

        # Check for failures
        if 'Failed' in response and response['Failed']:
            logger.warning(f"Some batch events failed: {response['Failed']}")

        logger.info(f"Published {len(entries)} events to SQS in batch")
        return response

    except ClientError as e:
        logger.exception(f"AWS SQS client error publishing batch events: {e}")
        return None

    except Exception as e:
        logger.exception(f"Unexpected error publishing batch events: {e}")
        return None


def purge_journey_queue(queue_url=None):
    """
    Purge all messages from the journey events queue
    Use with caution - this deletes ALL messages in the queue

    Args:
        queue_url: SQS queue URL (defaults to settings.JOURNEY_EVENTS_QUEUE_URL)

    Returns:
        bool: Success or failure
    """
    if not queue_url:
        queue_url = settings.JOURNEY_EVENTS_QUEUE_URL

    if not queue_url:
        logger.error("No queue URL provided or found in settings")
        return False

    try:
        sqs = boto3.client('sqs')

        # Purge the queue
        sqs.purge_queue(QueueUrl=queue_url)

        logger.warning(f"Purged all messages from queue: {queue_url}")
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
                'ApproximateNumberOfMessagesNotVisible',
                'ApproximateNumberOfMessagesDelayed',
                'CreatedTimestamp',
                'LastModifiedTimestamp',
                'VisibilityTimeout',
                'MaximumMessageSize',
                'MessageRetentionPeriod',
                'DelaySeconds',
                'ReceiveMessageWaitTimeSeconds'
            ]
        )

        attributes = response.get('Attributes', {})

        # Format the statistics
        statistics = {
            'queue_url': queue_url,
            'messages_available': int(attributes.get('ApproximateNumberOfMessages', 0)),
            'messages_in_flight': int(attributes.get('ApproximateNumberOfMessagesNotVisible', 0)),
            'messages_delayed': int(attributes.get('ApproximateNumberOfMessagesDelayed', 0)),
            'total_messages': int(attributes.get('ApproximateNumberOfMessages', 0)) +
                              int(attributes.get('ApproximateNumberOfMessagesNotVisible', 0)) +
                              int(attributes.get('ApproximateNumberOfMessagesDelayed', 0)),
            'visibility_timeout': int(attributes.get('VisibilityTimeout', 0)),
            'message_retention_period': int(attributes.get('MessageRetentionPeriod', 0)) / 86400,  # Convert to days
            'max_message_size': int(attributes.get('MaximumMessageSize', 0)) / 1024,  # Convert to KB
            'delay_seconds': int(attributes.get('DelaySeconds', 0)),
            'wait_time_seconds': int(attributes.get('ReceiveMessageWaitTimeSeconds', 0))
        }

        logger.debug(f"Queue statistics: {statistics}")
        return statistics

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
        failed_messages: List of tuples (message_id, receipt_handle, error)
        sqs_client: Boto3 SQS client (optional)
    """
    if not sqs_client:
        sqs_client = boto3.client('sqs')

    for message_id, receipt_handle, error in failed_messages:
        try:
            # Modify the visibility timeout to allow for retry
            # A shorter timeout will make the message available sooner for retry
            sqs_client.change_message_visibility(
                QueueUrl=queue_url,
                ReceiptHandle=receipt_handle,
                VisibilityTimeout=60  # 1 minute - adjust as needed for your use case
            )

            logger.info(f"Reset visibility timeout for failed message {message_id} to allow retry")

        except ClientError as e:
            logger.error(f"Could not change visibility for message {message_id}: {e}")


def _get_iso_timestamp():
    """Get current time as ISO 8601 string"""
    from datetime import datetime
    return datetime.utcnow().isoformat() + 'Z'  # Z indicates UTC