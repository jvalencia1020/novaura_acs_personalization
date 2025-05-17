import logging
from django.utils import timezone
from django.db import transaction
from django.db.models import Q

from external_models.models.nurturing_campaigns import (
    LeadNurturingCampaign,
    LeadNurturingParticipant,
    BulkCampaignMessage,
    DripCampaignSchedule,
    ReminderCampaignSchedule,
    BlastCampaignSchedule
)

logger = logging.getLogger(__name__)

class BulkCampaignProcessor:
    """
    Service class for processing bulk nurturing campaigns
    Handles drip campaigns, reminder campaigns, and blast campaigns
    """

    def __init__(self):
        self.campaign_processors = {
            'drip': self._process_drip_campaign,
            'reminder': self._process_reminder_campaign,
            'blast': self._process_blast_campaign
        }

    def process_campaign(self, campaign):
        """
        Process a bulk nurturing campaign based on its type

        Args:
            campaign: LeadNurturingCampaign instance

        Returns:
            int: Number of messages scheduled/sent
        """
        if not campaign.is_active_or_scheduled():
            logger.debug(f"Skipping inactive campaign {campaign}")
            return 0

        processor = self.campaign_processors.get(campaign.campaign_type)
        if not processor:
            logger.error(f"No processor found for campaign type: {campaign.campaign_type}")
            return 0

        return processor(campaign)

    def process_due_messages(self):
        """
        Process all messages that are due to be sent
        This should be run periodically by a scheduled task

        Returns:
            int: Number of messages processed
        """
        logger.info("Processing due messages...")

        # Find all pending messages that are due
        due_messages = BulkCampaignMessage.objects.filter(
            status__in=['pending', 'scheduled'],
            scheduled_for__lte=timezone.now()
        ).select_related(
            'campaign',
            'participant',
            'participant__lead'
        )

        processed_count = 0

        for message in due_messages:
            try:
                if self._send_message(message):
                    processed_count += 1
            except Exception as e:
                logger.exception(f"Error processing message {message.id}: {e}")

        logger.info(f"Processed {processed_count} due messages")
        return processed_count

    def _process_drip_campaign(self, campaign):
        """Process a drip campaign"""
        if not campaign.drip_schedule:
            logger.error(f"Drip campaign {campaign.id} has no schedule")
            return 0

        schedule = campaign.drip_schedule
        now = timezone.now()

        # Find active participants that need messages
        participants = LeadNurturingParticipant.objects.filter(
            nurturing_campaign=campaign,
            status='active'
        ).select_related('lead')

        scheduled_count = 0

        for participant in participants:
            # Check if participant has reached max messages
            if participant.messages_sent_count >= schedule.max_messages:
                continue

            # Check if it's time for next message
            if not self._should_send_drip_message(participant, schedule):
                continue

            # Schedule next message
            if self._schedule_drip_message(participant, schedule):
                scheduled_count += 1

        return scheduled_count

    def _process_reminder_campaign(self, campaign):
        """Process a reminder campaign"""
        if not campaign.reminder_schedule:
            logger.error(f"Reminder campaign {campaign.id} has no schedule")
            return 0

        schedule = campaign.reminder_schedule
        now = timezone.now()

        # Find active participants that need reminders
        participants = LeadNurturingParticipant.objects.filter(
            nurturing_campaign=campaign,
            status='active'
        ).select_related('lead')

        scheduled_count = 0

        for participant in participants:
            # Find next reminder time
            next_reminder = self._get_next_reminder_time(participant, schedule)
            if not next_reminder:
                continue

            # Schedule reminder
            if self._schedule_reminder_message(participant, next_reminder):
                scheduled_count += 1

        return scheduled_count

    def _process_blast_campaign(self, campaign):
        """Process a blast campaign"""
        if not campaign.blast_schedule:
            logger.error(f"Blast campaign {campaign.id} has no schedule")
            return 0

        schedule = campaign.blast_schedule
        now = timezone.now()

        # Check if it's time to send the blast
        if schedule.send_time > now:
            return 0

        # Find active participants that haven't received the blast
        participants = LeadNurturingParticipant.objects.filter(
            nurturing_campaign=campaign,
            status='active'
        ).exclude(
            bulk_messages__campaign=campaign
        ).select_related('lead')

        scheduled_count = 0

        for participant in participants:
            # Schedule blast message
            if self._schedule_blast_message(participant, schedule):
                scheduled_count += 1

        return scheduled_count

    def _should_send_drip_message(self, participant, schedule):
        """Check if a drip message should be sent to participant"""
        if not participant.last_message_sent_at:
            return True

        # Check if enough time has passed since last message
        interval = schedule.interval * 3600  # Convert hours to seconds
        elapsed = timezone.now() - participant.last_message_sent_at
        return elapsed.total_seconds() >= interval

    def _get_next_reminder_time(self, participant, schedule):
        """Get the next reminder time for a participant"""
        # Get all reminder times
        reminder_times = schedule.reminder_times.all().order_by(
            'days_before', 'days_before_relative', 'hours_before', 'minutes_before'
        )

        # Find the first reminder that hasn't been sent
        sent_days = set(
            participant.reminder_campaign_progress.values_list('days_before', flat=True)
        )

        for reminder in reminder_times:
            if reminder.days_before not in sent_days:
                return reminder

        return None

    def _schedule_drip_message(self, participant, schedule):
        """Schedule a drip campaign message"""
        try:
            with transaction.atomic():
                # Create message
                message = BulkCampaignMessage.objects.create(
                    campaign=participant.nurturing_campaign,
                    participant=participant,
                    status='scheduled',
                    scheduled_for=self._get_next_send_time(schedule)
                )

                # Update participant progress
                participant.update_campaign_progress(
                    scheduled_time=message.scheduled_for
                )

                return True
        except Exception as e:
            logger.exception(f"Error scheduling drip message: {e}")
            return False

    def _schedule_reminder_message(self, participant, reminder):
        """Schedule a reminder campaign message"""
        try:
            with transaction.atomic():
                # Calculate send time based on reminder settings
                send_time = self._calculate_reminder_time(reminder)

                # Create message
                message = BulkCampaignMessage.objects.create(
                    campaign=participant.nurturing_campaign,
                    participant=participant,
                    status='scheduled',
                    scheduled_for=send_time
                )

                # Update participant progress
                participant.update_campaign_progress(
                    scheduled_time=message.scheduled_for
                )

                return True
        except Exception as e:
            logger.exception(f"Error scheduling reminder message: {e}")
            return False

    def _schedule_blast_message(self, participant, schedule):
        """Schedule a blast campaign message"""
        try:
            with transaction.atomic():
                # Create message
                message = BulkCampaignMessage.objects.create(
                    campaign=participant.nurturing_campaign,
                    participant=participant,
                    status='scheduled',
                    scheduled_for=schedule.send_time
                )

                # Update participant progress
                participant.update_campaign_progress(
                    scheduled_time=message.scheduled_for
                )

                return True
        except Exception as e:
            logger.exception(f"Error scheduling blast message: {e}")
            return False

    def _send_message(self, message):
        """Send a scheduled message"""
        try:
            # Get campaign and participant
            campaign = message.campaign
            participant = message.participant

            # Check if message can be sent
            if not campaign.can_send_message(participant):
                logger.debug(f"Cannot send message {message.id} - campaign or participant not active")
                return False

            # Send message based on channel
            if campaign.channel == 'email':
                success = self._send_email(message)
            elif campaign.channel == 'sms':
                success = self._send_sms(message)
            elif campaign.channel == 'voice':
                success = self._send_voice(message)
            elif campaign.channel == 'chat':
                success = self._send_chat(message)
            else:
                logger.error(f"Unsupported channel: {campaign.channel}")
                return False

            if success:
                # Update message status
                message.update_status('sent')
                
                # Update participant progress
                participant.update_campaign_progress(message_sent=True)

                return True

            return False

        except Exception as e:
            logger.exception(f"Error sending message {message.id}: {e}")
            message.update_status('failed', {'error': str(e)})
            return False

    def _get_next_send_time(self, schedule):
        """Calculate the next time a message should be sent based on schedule"""
        now = timezone.now()
        
        # If outside business hours, move to next business day
        if schedule.business_hours_only:
            if now.time() >= schedule.end_time:
                # Move to next day
                next_day = now + timezone.timedelta(days=1)
                if schedule.exclude_weekends and next_day.weekday() >= 5:
                    # Skip weekend
                    next_day += timezone.timedelta(days=2)
                return timezone.datetime.combine(next_day.date(), schedule.start_time)
            elif now.time() < schedule.start_time:
                # Move to start time today
                return timezone.datetime.combine(now.date(), schedule.start_time)

        return now

    def _calculate_reminder_time(self, reminder):
        """Calculate the send time for a reminder"""
        now = timezone.now()

        if reminder.days_before is not None:
            # Absolute scheduling
            send_date = now.date() + timezone.timedelta(days=reminder.days_before)
            if reminder.time:
                return timezone.datetime.combine(send_date, reminder.time)
            return timezone.datetime.combine(send_date, time(9, 0))  # Default to 9 AM
        else:
            # Relative scheduling
            total_seconds = 0
            if reminder.days_before_relative:
                total_seconds += reminder.days_before_relative * 86400
            if reminder.hours_before:
                total_seconds += reminder.hours_before * 3600
            if reminder.minutes_before:
                total_seconds += reminder.minutes_before * 60

            return now + timezone.timedelta(seconds=total_seconds)

    def _send_email(self, message):
        """Send an email message"""
        # TODO: Implement email sending
        return True

    def _send_sms(self, message):
        """Send an SMS message"""
        # TODO: Implement SMS sending
        return True

    def _send_voice(self, message):
        """Send a voice message"""
        # TODO: Implement voice message sending
        return True

    def _send_chat(self, message):
        """Send a chat message"""
        # TODO: Implement chat message sending
        return True 