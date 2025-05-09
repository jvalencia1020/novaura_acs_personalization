# journey_processor/tasks/scheduled_tasks.py
import logging
from django.utils import timezone
from django.db.models import Q

from acs.models import (
    Journey, JourneyStep, JourneyStepConnection,
    JourneyParticipant, LeadNurturingCampaign
)
from journey_processor.services.journey_processor import JourneyProcessor

logger = logging.getLogger(__name__)


def process_timed_connections():
    """
    Process all time-based connections that should trigger now
    """
    processor = JourneyProcessor()
    processed_count = processor.process_timed_connections()

    logger.info(f"Processed {processed_count} timed connections")
    return processed_count


def enroll_leads_in_journeys():
    """
    Enroll leads in journeys based on campaign criteria
    """
    from crm.models import Lead

    # Get active nurturing campaigns
    active_campaigns = LeadNurturingCampaign.objects.filter(
        active=True,
        journey__is_active=True
    ).select_related('journey')

    total_enrollments = 0

    for campaign in active_campaigns:
        journey = campaign.journey

        # Check campaign scheduling
        if not _is_campaign_scheduled_now(campaign):
            continue

        # Get campaign configuration
        config = campaign.config or {}
        enrollment_criteria = config.get('enrollment_criteria', {})

        # Build query based on criteria
        lead_query = Q(status='active')  # Base query for active leads

        # Add criteria for funnel
        if journey.funnel_id:
            lead_query &= Q(funnel_id=journey.funnel_id)

        # Add criteria for specific funnel steps if defined
        funnel_steps = enrollment_criteria.get('funnel_steps', [])
        if funnel_steps:
            lead_query &= Q(current_step__in=funnel_steps)

        # Add campaign-specific filtering
        campaign_id = journey.campaign_id
        if campaign_id:
            lead_query &= Q(campaign_id=campaign_id)

        # Custom field criteria
        field_criteria = enrollment_criteria.get('fields', [])
        for criteria in field_criteria:
            field = criteria.get('field')
            operator = criteria.get('operator', 'eq')
            value = criteria.get('value')

            if not field or value is None:
                continue

            # Handle different operators
            if operator == 'eq':
                lead_query &= Q(**{field: value})
            elif operator == 'neq':
                lead_query &= ~Q(**{field: value})
            elif operator == 'contains':
                lead_query &= Q(**{f"{field}__icontains": value})
            elif operator == 'gt':
                lead_query &= Q(**{f"{field}__gt": value})
            elif operator == 'lt':
                lead_query &= Q(**{f"{field}__lt": value})
            # Add more operators as needed

        # Exclude leads already in this journey
        existing_participants = JourneyParticipant.objects.filter(
            journey=journey
        ).values_list('lead_id', flat=True)

        lead_query &= ~Q(id__in=existing_participants)

        # Get eligible leads
        eligible_leads = Lead.objects.filter(lead_query)

        # Apply enrollment limit if configured
        max_enrollments = config.get('max_enrollments_per_run')
        if max_enrollments and max_enrollments > 0:
            eligible_leads = eligible_leads[:max_enrollments]

        # Create journey participants for eligible leads
        processor = JourneyProcessor()
        enrollment_count = 0

        for lead in eligible_leads:
            try:
                # Create the participant
                participant = JourneyParticipant.objects.create(
                    lead=lead,
                    journey=journey,
                    nurturing_campaign=campaign,
                    status='active',
                    created_by_id=campaign.created_by_id,
                    last_updated_by_id=campaign.created_by_id
                )

                # Process the new participant (assign to entry point)
                processor.process_participant(participant)

                enrollment_count += 1

            except Exception as e:
                logger.exception(f"Error enrolling lead {lead.id} in journey {journey.id}: {e}")

        logger.info(f"Enrolled {enrollment_count} leads in journey: {journey.name}")
        total_enrollments += enrollment_count

    return total_enrollments


def _is_campaign_scheduled_now(campaign):
    """Check if a campaign is scheduled to run at the current time"""
    now = timezone.now()

    # Check start/end dates
    if campaign.start_date and now < campaign.start_date:
        return False

    if campaign.end_date and now > campaign.end_date:
        return False

    # Check advanced scheduling settings if available
    schedule_settings = campaign.schedule_settings or {}

    # Time of day restrictions
    if 'time_window' in schedule_settings:
        window = schedule_settings['time_window']
        start_time = window.get('start_time')
        end_time = window.get('end_time')

        if start_time and end_time:
            start_hour, start_minute = map(int, start_time.split(':'))
            end_hour, end_minute = map(int, end_time.split(':'))

            current_hour = now.hour
            current_minute = now.minute

            # Convert to minutes for easier comparison
            current_time_mins = current_hour * 60 + current_minute
            start_time_mins = start_hour * 60 + start_minute
            end_time_mins = end_hour * 60 + end_minute

            if not (start_time_mins <= current_time_mins <= end_time_mins):
                return False

    # Day of week restrictions
    if 'days_of_week' in schedule_settings:
        allowed_days = schedule_settings['days_of_week']
        current_day = now.strftime('%A').lower()

        if allowed_days and current_day not in allowed_days:
            return False

    return True