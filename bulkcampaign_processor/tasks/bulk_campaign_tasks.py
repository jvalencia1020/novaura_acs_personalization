import logging
from django.utils import timezone
from django.db.models import Q

from external_models.models.nurturing_campaigns import LeadNurturingCampaign
from bulkcampaign_processor.services.bulk_campaign_processor import BulkCampaignProcessor

logger = logging.getLogger(__name__)

def process_bulk_campaigns():
    """
    Process all active bulk campaigns
    This should be run periodically by a scheduled task
    """
    processor = BulkCampaignProcessor()
    processed_count = 0

    # Find all active bulk campaigns
    campaigns = LeadNurturingCampaign.objects.filter(
        Q(status='active') | Q(status='scheduled'),
        campaign_type__in=['drip', 'reminder', 'blast']
    ).select_related(
        'drip_schedule',
        'reminder_schedule',
        'blast_schedule'
    )

    for campaign in campaigns:
        try:
            count = processor.process_campaign(campaign)
            processed_count += count
            logger.info(f"Processed campaign {campaign.id}: {count} messages scheduled")
        except Exception as e:
            logger.exception(f"Error processing campaign {campaign.id}: {e}")

    logger.info(f"Processed {processed_count} total messages across {campaigns.count()} campaigns")
    return processed_count

def process_due_messages():
    """
    Process all messages that are due to be sent
    This should be run frequently (e.g., every minute) by a scheduled task
    """
    processor = BulkCampaignProcessor()
    processed_count = processor.process_due_messages()
    return processed_count 