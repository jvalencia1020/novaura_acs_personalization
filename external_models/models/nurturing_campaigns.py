from django.db import models
from django.conf import settings
from django.core.exceptions import ValidationError
from django.utils import timezone
import pytz
from datetime import datetime, timedelta
from .external_references import Account, Campaign, Lead

class LeadNurturingCampaign(models.Model):
    CAMPAIGN_TYPES = [
        ('journey', 'Journey Based'),
        ('drip', 'Drip Campaign'),
        ('reminder', 'Reminder Campaign'),
        ('blast', 'One-time Blast'),
    ]

    CHANNEL_TYPES = [
        ('email', 'Email'),
        ('sms', 'SMS'),
        ('voice', 'Voice'),
        ('chat', 'Chat'),
    ]

    CAMPAIGN_STATUS_CHOICES = [
        ('draft', 'Draft'),
        ('scheduled', 'Scheduled'),
        ('active', 'Active'),
        ('paused', 'Paused'),
        ('completed', 'Completed'),
        ('cancelled', 'Cancelled'),
    ]

    # Account relationship
    account = models.ForeignKey(Account, on_delete=models.CASCADE, related_name='nurturing_campaigns')
    
    # Existing fields
    journey = models.ForeignKey('Journey', on_delete=models.CASCADE, related_name='nurturing_campaigns', null=True, blank=True)
    name = models.CharField(max_length=255)
    description = models.TextField(blank=True, null=True)
    active = models.BooleanField(default=True)
    start_date = models.DateTimeField(null=True, blank=True)
    end_date = models.DateTimeField(null=True, blank=True)
    is_ongoing = models.BooleanField(
        default=False,
        help_text="If True, campaign runs indefinitely until manually ended"
    )
    status = models.CharField(
        max_length=20,
        choices=CAMPAIGN_STATUS_CHOICES,
        default='draft',
        help_text="Current status of the campaign"
    )
    status_changed_at = models.DateTimeField(null=True, blank=True)
    status_changed_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        related_name='status_changed_campaigns'
    )
    
    # Auto-enrollment configuration
    auto_enroll_new_leads = models.BooleanField(
        default=False,
        help_text="If True, automatically enroll new leads that match the criteria"
    )
    auto_enroll_filters = models.JSONField(
        blank=True, 
        null=True,
        help_text="Filters to determine which new leads should be auto-enrolled"
    )
    
    config = models.JSONField(
        blank=True, 
        null=True,
        help_text="Additional configuration for the campaign"
    )
    created_by = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    # New fields for campaign type
    campaign_type = models.CharField(max_length=20, choices=CAMPAIGN_TYPES, default='journey')
    channel = models.CharField(
        max_length=10,
        choices=CHANNEL_TYPES,
        null=True,
        blank=True
    )
    template = models.ForeignKey(
        'MessageTemplate',
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='bulk_campaigns'
    )
    content = models.TextField(blank=True, null=True)
    crm_campaign = models.ForeignKey(Campaign, on_delete=models.SET_NULL, null=True, blank=True, related_name='nurturing_campaigns')

    class Meta:
        managed = False
        db_table = 'acs_leadnurturingcampaign'
        indexes = [
            models.Index(fields=['status']),
            models.Index(fields=['is_ongoing']),
            models.Index(fields=['status_changed_at']),
        ]

    def clean(self):
        """Validate campaign configuration"""
        super().clean()
        
        if not self.is_ongoing and self.start_date and self.end_date and self.start_date > self.end_date:
            raise ValidationError("End date must be after start date")

        if self.campaign_type == 'journey':
            if not self.journey:
                raise ValidationError("Journey is required for journey-based campaigns")
        else:  # bulk campaign
            if not self.channel:
                raise ValidationError("Channel is required for bulk campaigns")
            if not self.template and not self.content:
                raise ValidationError("Either template or content is required for bulk campaigns")

    def update_status(self, new_status, user):
        """Update the campaign status with proper tracking"""
        if new_status not in dict(self.CAMPAIGN_STATUS_CHOICES):
            raise ValueError(f"Invalid status: {new_status}")

        if new_status in ['completed', 'cancelled']:
            self.is_ongoing = False
            if not self.end_date:
                self.end_date = timezone.now()

        self.status = new_status
        self.status_changed_at = timezone.now()
        self.status_changed_by = user
        self.save()

    def is_active_or_scheduled(self):
        """Check if campaign is currently active or scheduled to start"""
        if not self.active:
            return False

        if self.status not in ['active', 'scheduled']:
            return False

        now = timezone.now()
        
        if self.start_date and self.start_date > now:
            return False

        if not self.is_ongoing and self.end_date and self.end_date < now:
            return False

        return True

    def can_send_message(self, participant):
        """Check if a message can be sent to a participant"""
        if not self.is_active_or_scheduled():
            return False

        if participant.status not in ['active']:
            return False

        if self.is_ongoing:
            return not self.start_date or self.start_date <= timezone.now()

        now = timezone.now()
        if self.start_date and self.start_date > now:
            return False
        if self.end_date and self.end_date < now:
            return False

        return True

class CampaignScheduleBase(models.Model):
    """Base model for campaign scheduling"""
    business_hours_only = models.BooleanField(default=False)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        abstract = True
        managed = False

class DripCampaignSchedule(CampaignScheduleBase):
    """Schedule settings for drip campaigns"""
    campaign = models.OneToOneField(LeadNurturingCampaign, on_delete=models.CASCADE, related_name='drip_schedule')
    interval = models.PositiveIntegerField(help_text="Interval in hours")
    max_messages = models.PositiveIntegerField()
    start_time = models.TimeField()
    end_time = models.TimeField()
    exclude_weekends = models.BooleanField(default=False)

    def clean(self):
        super().clean()
        if self.start_time >= self.end_time:
            raise ValidationError("End time must be after start time")

    class Meta:
        managed = False
        db_table = 'acs_dripcampaignschedule'

class ReminderCampaignSchedule(CampaignScheduleBase):
    """Schedule settings for reminder campaigns"""
    campaign = models.OneToOneField(LeadNurturingCampaign, on_delete=models.CASCADE, related_name='reminder_schedule')
    use_relative_schedule = models.BooleanField(
        default=False,
        help_text="If True, reminders will be scheduled relative to appointment time"
    )

    class Meta:
        managed = False
        db_table = 'acs_remindercampaignschedule'

class ReminderTime(models.Model):
    """Individual reminder times for reminder campaigns"""
    schedule = models.ForeignKey(ReminderCampaignSchedule, on_delete=models.CASCADE, related_name='reminder_times')
    days_before = models.PositiveIntegerField(null=True, blank=True)
    time = models.TimeField(null=True, blank=True)
    days_before_relative = models.PositiveIntegerField(null=True, blank=True)
    hours_before = models.PositiveIntegerField(null=True, blank=True)
    minutes_before = models.PositiveIntegerField(null=True, blank=True)

    class Meta:
        managed = False
        db_table = 'acs_remindertime'
        ordering = ['days_before', 'days_before_relative', 'hours_before', 'minutes_before']
        unique_together = [
            ['schedule', 'days_before', 'time'],
            ['schedule', 'days_before_relative', 'hours_before', 'minutes_before']
        ]

    def clean(self):
        super().clean()
        absolute_fields = bool(self.days_before is not None and self.time is not None)
        relative_fields = bool(
            self.days_before_relative is not None or 
            self.hours_before is not None or 
            self.minutes_before is not None
        )
        
        if absolute_fields and relative_fields:
            raise ValidationError(
                "Cannot mix absolute and relative scheduling"
            )

class BlastCampaignSchedule(CampaignScheduleBase):
    """Schedule settings for blast campaigns"""
    campaign = models.OneToOneField(LeadNurturingCampaign, on_delete=models.CASCADE, related_name='blast_schedule')
    send_time = models.DateTimeField()
    timezone = models.CharField(max_length=50, null=True, blank=True)

    class Meta:
        managed = False
        db_table = 'acs_blastcampaignschedule'

class JourneyCampaignSchedule(CampaignScheduleBase):
    """Schedule settings for journey-based campaigns"""
    campaign = models.OneToOneField(LeadNurturingCampaign, on_delete=models.CASCADE, related_name='journey_schedule')
    start_time = models.TimeField(null=True, blank=True)
    end_time = models.TimeField(null=True, blank=True)
    exclude_weekends = models.BooleanField(default=False)
    exclude_holidays = models.BooleanField(default=False)
    min_step_delay = models.PositiveIntegerField(default=0)
    max_steps_per_day = models.PositiveIntegerField(null=True, blank=True)
    max_retry_attempts = models.PositiveIntegerField(default=3)
    retry_delay_minutes = models.PositiveIntegerField(default=60)
    step_timeout_minutes = models.PositiveIntegerField(default=1440)
    allow_parallel_steps = models.BooleanField(default=False)
    max_parallel_steps = models.PositiveIntegerField(default=1)
    timezone = models.CharField(max_length=50, null=True, blank=True)

    def clean(self):
        super().clean()
        if self.start_time and self.end_time and self.start_time >= self.end_time:
            raise ValidationError("End time must be after start time")
        if self.allow_parallel_steps and self.max_parallel_steps < 1:
            raise ValidationError("max_parallel_steps must be at least 1")
        if self.max_steps_per_day is not None and self.max_steps_per_day < 1:
            raise ValidationError("max_steps_per_day must be at least 1")
        if self.max_retry_attempts < 1:
            raise ValidationError("max_retry_attempts must be at least 1")
        if self.retry_delay_minutes < 1:
            raise ValidationError("retry_delay_minutes must be at least 1")
        if self.step_timeout_minutes < 1:
            raise ValidationError("step_timeout_minutes must be at least 1")

    class Meta:
        managed = False
        db_table = 'asc_journeycampaignschedule'

class BulkCampaignMessage(models.Model):
    STATUS_CHOICES = [
        ('pending', 'Pending'),
        ('scheduled', 'Scheduled'),
        ('sent', 'Sent'),
        ('delivered', 'Delivered'),
        ('failed', 'Failed'),
        ('opened', 'Opened'),
        ('clicked', 'Clicked'),
        ('replied', 'Replied'),
        ('opted_out', 'Opted Out')
    ]

    campaign = models.ForeignKey(LeadNurturingCampaign, on_delete=models.CASCADE, related_name='messages')
    participant = models.ForeignKey('LeadNurturingParticipant', on_delete=models.CASCADE, related_name='bulk_messages')
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='pending')
    scheduled_for = models.DateTimeField(null=True, blank=True)
    sent_at = models.DateTimeField(null=True, blank=True)
    delivered_at = models.DateTimeField(null=True, blank=True)
    opened_at = models.DateTimeField(null=True, blank=True)
    clicked_at = models.DateTimeField(null=True, blank=True)
    replied_at = models.DateTimeField(null=True, blank=True)
    error_message = models.TextField(blank=True, null=True)
    metadata = models.JSONField(blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        managed = False
        db_table = 'bulk_campaign_message'
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['campaign', 'status']),
            models.Index(fields=['participant', 'status']),
            models.Index(fields=['scheduled_for']),
        ]

    def update_status(self, new_status, metadata=None):
        """Update message status and related timestamps"""
        self.status = new_status
        now = timezone.now()

        if new_status == 'sent':
            self.sent_at = now
        elif new_status == 'delivered':
            self.delivered_at = now
        elif new_status == 'opened':
            self.opened_at = now
        elif new_status == 'clicked':
            self.clicked_at = now
        elif new_status == 'replied':
            self.replied_at = now

        if metadata:
            if not self.metadata:
                self.metadata = {}
            self.metadata.update(metadata)

        self.save()

    def can_be_sent(self):
        """Check if the message can be sent"""
        if self.status not in ['pending', 'scheduled']:
            return False

        if self.scheduled_for and self.scheduled_for > timezone.now():
            return False

        return self.campaign.can_send_message(self.participant)

class LeadNurturingParticipant(models.Model):
    lead = models.ForeignKey(Lead, on_delete=models.CASCADE, related_name='lead_nurturing_participations')
    nurturing_campaign = models.ForeignKey('LeadNurturingCampaign', on_delete=models.CASCADE, related_name='participants')
    current_journey_step = models.ForeignKey('JourneyStep', on_delete=models.SET_NULL, null=True, blank=True, related_name='current_participants')
    status = models.CharField(
        max_length=20,
        choices=[
            ('active', 'Active'),
            ('completed', 'Completed'),
            ('exited', 'Exited'),
            ('paused', 'Paused'),
            ('opted_out', 'Opted Out')
        ],
        default='active'
    )
    last_event_at = models.DateTimeField(null=True, blank=True)
    entered_campaign_at = models.DateTimeField(auto_now_add=True)
    exited_campaign_at = models.DateTimeField(null=True, blank=True)
    updated_at = models.DateTimeField(auto_now=True)
    created_by = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE, related_name='created_lead_nurturing_participants')
    last_updated_by = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True, related_name='updated_lead_nurturing_participants')
    
    # Campaign tracking fields
    last_message_sent_at = models.DateTimeField(null=True, blank=True)
    messages_sent_count = models.PositiveIntegerField(default=0)
    next_scheduled_message = models.DateTimeField(null=True, blank=True)

    class Meta:
        managed = False
        db_table = 'lead_nurturing_participant'
        unique_together = [
            ['lead', 'nurturing_campaign', 'status']
        ]
        indexes = [
            models.Index(fields=['status']),
            models.Index(fields=['last_event_at']),
            models.Index(fields=['last_message_sent_at']),
            models.Index(fields=['next_scheduled_message']),
            models.Index(fields=['entered_campaign_at']),
            models.Index(fields=['exited_campaign_at']),
        ]

    def __str__(self):
        return f"{self.lead} in {self.nurturing_campaign}"

    def clean(self):
        """Validate participant configuration"""
        super().clean()
        
        # For journey-based campaigns, journey is required
        if self.nurturing_campaign and self.nurturing_campaign.campaign_type == 'journey' and not self.nurturing_campaign.journey:
            raise ValidationError("Journey is required for journey-based campaigns")

    def move_to_next_step(self, next_step, event_type='enter_step', metadata=None):
        """Move participant to next step and create event"""
        if not self.nurturing_campaign.journey:
            raise ValidationError("Cannot move to next step for bulk campaigns")
            
        self.current_journey_step = next_step
        self.last_event_at = timezone.now()
        self.save()
        
        JourneyEvent.objects.create(
            participant=self,
            journey_step=next_step,
            event_type=event_type,
            metadata=metadata,
            created_by=self.last_updated_by
        )

    def update_campaign_progress(self, message_sent=False, scheduled_time=None):
        """Update campaign progress for bulk campaigns"""
        if not self.nurturing_campaign or self.nurturing_campaign.campaign_type == 'journey':
            return

        now = timezone.now()
        campaign = self.nurturing_campaign

        if message_sent:
            self.messages_sent_count += 1
            self.last_message_sent_at = now

        if scheduled_time:
            self.next_scheduled_message = scheduled_time

        self.save()

        # Update campaign-specific progress
        if campaign.campaign_type == 'drip':
            self._update_drip_progress(now, scheduled_time)
        elif campaign.campaign_type == 'reminder':
            self._update_reminder_progress(now, scheduled_time)
        elif campaign.campaign_type == 'blast':
            self._update_blast_progress(now)

    def _update_drip_progress(self, now, scheduled_time):
        """Update progress for drip campaigns"""
        progress, created = DripCampaignProgress.objects.get_or_create(
            participant=self,
            defaults={
                'total_intervals': self.nurturing_campaign.drip_schedule.max_messages
            }
        )
        
        if self.messages_sent_count > 0:
            progress.last_interval = now
            progress.intervals_completed = self.messages_sent_count
        
        if scheduled_time:
            progress.next_scheduled_interval = scheduled_time
            
        progress.save()

    def _update_reminder_progress(self, now, scheduled_time):
        """Update progress for reminder campaigns"""
        days_before = self._get_days_before(now)
        if days_before > 0:
            ReminderCampaignProgress.objects.create(
                participant=self,
                days_before=days_before,
                sent_at=now,
                next_scheduled_reminder=scheduled_time
            )

    def _update_blast_progress(self, now):
        """Update progress for blast campaigns"""
        progress, created = BlastCampaignProgress.objects.get_or_create(
            participant=self
        )
        progress.message_sent = True
        progress.sent_at = now
        progress.save()

    def _get_days_before(self, current_time):
        """Helper method to calculate days before for reminder campaigns"""
        if not self.nurturing_campaign or self.nurturing_campaign.campaign_type != 'reminder':
            return 0

        campaign = self.nurturing_campaign
        if not campaign.reminder_schedule:
            return 0

        # Find the next reminder time that hasn't been sent yet
        sent_days = set(
            self.reminder_campaign_progress.values_list('days_before', flat=True)
        )
        
        for reminder in campaign.reminder_schedule.reminder_times.all():
            if reminder.days_before not in sent_days:
                return reminder.days_before

        return 0

    def get_campaign_progress(self):
        """Get the current campaign progress"""
        campaign = self.nurturing_campaign
        if not campaign:
            return None

        if campaign.campaign_type == 'drip':
            progress = self.drip_campaign_progress.first()
            if progress:
                return {
                    'last_interval': progress.last_interval,
                    'intervals_completed': progress.intervals_completed,
                    'total_intervals': progress.total_intervals,
                    'next_scheduled_interval': progress.next_scheduled_interval
                }
        elif campaign.campaign_type == 'reminder':
            reminders = self.reminder_campaign_progress.all().order_by('-sent_at')
            next_reminder = reminders.filter(next_scheduled_reminder__isnull=False).first()
            return {
                'reminders_sent': [
                    {
                        'days_before': r.days_before,
                        'sent_at': r.sent_at
                    } for r in reminders
                ],
                'next_reminder': {
                    'days_before': next_reminder.days_before if next_reminder else None,
                    'scheduled_for': next_reminder.next_scheduled_reminder if next_reminder else None
                } if next_reminder else None
            }
        elif campaign.campaign_type == 'blast':
            progress = self.blast_campaign_progress.first()
            if progress:
                return {
                    'message_sent': progress.message_sent,
                    'sent_at': progress.sent_at
                }

        return None
