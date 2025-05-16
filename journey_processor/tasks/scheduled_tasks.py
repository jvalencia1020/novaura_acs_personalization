# journey_processor/tasks/scheduled_tasks.py
import logging
from django.utils import timezone
from django.db.models import Q

from external_models.models.journeys import Journey, JourneyStep, JourneyStepConnection
from external_models.models.nurturing_campaigns import LeadNurturingParticipant, LeadNurturingCampaign
from journey_processor.services.journey_processor import JourneyProcessor

logger = logging.getLogger(__name__)


def process_timed_connections():
    """
    Process all delay-based connections that should trigger now
    """
    processor = JourneyProcessor()
    processed_count = processor.process_timed_connections()

    logger.info(f"Processed {processed_count} timed connections")
    return processed_count