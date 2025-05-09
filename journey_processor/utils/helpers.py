# journey_processor/utils/helpers.py

import logging
import json
import boto3
from django.conf import settings

logger = logging.getLogger(__name__)


def publish_journey_event(event_type, data):
    """
    Publish a journey event to SQS

    This can be used to trigger journey processing from other parts of the service

    Args:
        event_type: Event type identifier
        data: Event data

    Returns:
        dict: SQS response or None on error
    """
    queue_url = settings.JOURNEY_EVENTS_QUEUE_URL

    if not queue_url:
        logger.error("JOURNEY_EVENTS_QUEUE_URL not set - cannot publish journey event")
        return None

    try:
        sqs = boto3.client('sqs')

        message_body = json.dumps({
            'event_type': event_type,
            'data': data
        })

        response = sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=message_body
        )

        logger.debug(f"Published {event_type} event to SQS: {response['MessageId']}")
        return response
    except Exception as e:
        logger.exception(f"Error publishing journey event: {e}")
        return None


def format_datetime_for_display(dt):
    """Format a datetime for display"""
    if not dt:
        return ""
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def get_delay_display_text(connection):
    """Get human-readable text for a delay connection"""
    if not connection or connection.trigger_type != 'delay':
        return ""

    duration = connection.delay_duration
    unit = connection.delay_unit

    if not duration or not unit:
        return ""

    # Format for single units
    if duration == 1:
        if unit.endswith('s'):  # Remove plural 's'
            unit = unit[:-1]

    return f"{duration} {unit}"