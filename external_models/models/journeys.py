from django.db import models
from django.conf import settings
from django.core.exceptions import ValidationError
from django.utils import timezone
from .external_references import Account, Campaign, Funnel, Step

class Journey(models.Model):
    account = models.ForeignKey(Account, on_delete=models.CASCADE, related_name='journeys')
    name = models.CharField(max_length=255)
    description = models.TextField(blank=True, null=True)
    funnel = models.ForeignKey(Funnel, on_delete=models.CASCADE, related_name='journeys')
    campaign = models.ForeignKey(Campaign, on_delete=models.CASCADE, related_name='journeys')
    created_by = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE, related_name='created_journeys')
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    is_active = models.BooleanField(default=True)
    start_date = models.DateTimeField(null=True, blank=True)
    end_date = models.DateTimeField(null=True, blank=True)

    class Meta:
        managed = False
        db_table = 'journey'
        ordering = ['-created_at']

    def clean(self):
        super().clean()
        if self.start_date and self.end_date and self.start_date > self.end_date:
            raise ValidationError("End date must be after start date")

    def get_active_participants(self):
        return self.participants.filter(status='active')

class JourneyStep(models.Model):
    journey = models.ForeignKey(Journey, on_delete=models.CASCADE, related_name='steps')
    name = models.CharField(max_length=100)
    order = models.PositiveIntegerField()
    step_type = models.CharField(
        max_length=50,
        choices=[
            ('email', 'Email'),
            ('sms', 'SMS'),
            ('voice', 'Voice Call'),
            ('chat', 'Chat Message'),
            ('wait_step', 'Wait Step'),
            ('validation_step', 'Validation Step'),
            ('goal', 'Goal'),
            ('webhook', 'Webhook'),
            ('end', 'End'),
        ]
    )
    template = models.ForeignKey(
        'MessageTemplate', 
        on_delete=models.SET_NULL, 
        null=True, 
        blank=True, 
        related_name='journey_steps'
    )
    config = models.JSONField(blank=True, null=True)
    is_entry_point = models.BooleanField(default=False)
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        managed = False
        db_table = 'journey_step'
        ordering = ['order']
        unique_together = ['journey', 'order']

    def clean(self):
        super().clean()
        if self.step_type in ['email', 'sms', 'voice', 'chat']:
            if not self.template and not self.config.get('content'):
                raise ValidationError(
                    f"{self.step_type.title()} steps must have either a template or direct content in config"
                )
        if self.step_type == 'wait_step' and not self.config.get('duration'):
            raise ValidationError("Wait steps must have a duration in config")
        if self.step_type == 'validation_step' and not self.config.get('validation_type'):
            raise ValidationError("Validation steps must have a validation_type in config")
        if self.step_type == 'webhook' and not self.config.get('url'):
            raise ValidationError("Webhook steps must have a URL in config")

class JourneyEvent(models.Model):
    participant = models.ForeignKey('LeadNurturingParticipant', on_delete=models.CASCADE, related_name='events')
    journey_step = models.ForeignKey(JourneyStep, on_delete=models.CASCADE, related_name='events')
    event_type = models.CharField(
        max_length=50,
        choices=[
            ('enter_step', 'Entered Step'),
            ('exit_step', 'Exited Step'),
            ('action_sent', 'Action Sent'),
            ('action_delivered', 'Action Delivered'),
            ('action_opened', 'Action Opened'),
            ('action_clicked', 'Action Clicked'),
            ('condition_met', 'Condition Met'),
            ('condition_not_met', 'Condition Not Met'),
            ('error', 'Error'),
        ]
    )
    event_timestamp = models.DateTimeField(auto_now_add=True)
    metadata = models.JSONField(blank=True, null=True)
    created_by = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE, related_name='created_journey_events')

    class Meta:
        managed = False
        db_table = 'journey_event'
        indexes = [
            models.Index(fields=['event_type']),
            models.Index(fields=['event_timestamp']),
        ] 

class JourneyStepConnection(models.Model):
    TRIGGER_TYPES = [
        ('immediate', 'Immediate'),
        ('delay', 'Time Delay'),
        ('funnel_change', 'Funnel Step Change'),
        ('event', 'Event Based'),
        ('condition', 'Condition Met'),
        ('manual', 'Manual Trigger'),
    ]
    
    EVENT_TYPES = [
        ('email_opened', 'Email Opened'),
        ('email_clicked', 'Email Clicked'),
        ('sms_delivered', 'SMS Delivered'),
        ('sms_replied', 'SMS Replied'),
        ('form_submitted', 'Form Submitted'),
        ('page_visited', 'Page Visited'),
        ('button_clicked', 'Button Clicked'),
        ('link_clicked', 'Link Clicked'),
        ('appointment_scheduled', 'Appointment Scheduled'),
        ('appointment_cancelled', 'Appointment Cancelled'),
        ('appointment_completed', 'Appointment Completed'),
        ('lead_created', 'Lead Created'),
        ('lead_updated', 'Lead Updated'),
        ('lead_converted', 'Lead Converted'),
        ('custom_event', 'Custom Event'),
    ]

    CONDITION_TYPES = [
        ('field_equals', 'Field Equals'),
        ('field_contains', 'Field Contains'),
        ('field_greater_than', 'Field Greater Than'),
        ('field_less_than', 'Field Less Than'),
        ('field_is_empty', 'Field Is Empty'),
        ('field_is_not_empty', 'Field Is Not Empty'),
    ]

    FIELD_SOURCES = [
        ('lead', 'Lead Model'),
        ('d2c_lead', 'D2C Lead Model'),
        ('b2b_lead', 'B2B Lead Model'),
        ('lead_field_value', 'Lead Field Value'),
        ('lead_intake_value', 'Lead Intake Value'),
        ('custom_field', 'Custom Field'),
    ]
    
    from_step = models.ForeignKey('JourneyStep', on_delete=models.CASCADE, related_name='next_connections')
    to_step = models.ForeignKey('JourneyStep', on_delete=models.CASCADE, related_name='previous_connections')
    
    # New fields for trigger control
    trigger_type = models.CharField(
        max_length=50, 
        choices=TRIGGER_TYPES,
        default='immediate'
    )
    
    # For delay triggers - stores duration in seconds
    delay_duration = models.PositiveIntegerField(null=True, blank=True, help_text="Delay duration in seconds")
    
    # Time unit for better UI representation (hours, days, etc.)
    delay_unit = models.CharField(
        max_length=20,
        choices=[
            ('seconds', 'Seconds'),
            ('minutes', 'Minutes'),
            ('hours', 'Hours'),
            ('days', 'Days'),
            ('weeks', 'Weeks')
        ],
        null=True, blank=True
    )
    
    # For funnel step changes
    funnel_step = models.ForeignKey(
        Step,
        on_delete=models.SET_NULL, 
        null=True, blank=True,
        related_name='journey_connections'
    )
    
    # For event-based triggers
    event_type = models.CharField(
        max_length=100, 
        choices=EVENT_TYPES,
        null=True, 
        blank=True,
        help_text="Type of event that triggers this connection"
    )
    
    # For condition triggers
    condition_label = models.CharField(
        max_length=255, 
        blank=True, 
        null=True,
        help_text="Human-readable label for this condition"
    )
    
    condition_type = models.CharField(
        max_length=50,
        choices=CONDITION_TYPES,
        null=True,
        blank=True,
        help_text="Type of condition to evaluate"
    )
    
    field_source = models.CharField(
        max_length=50,
        choices=FIELD_SOURCES,
        null=True,
        blank=True,
        help_text="Source model for the field to evaluate"
    )
    
    field_name = models.CharField(
        max_length=255,
        null=True,
        blank=True,
        help_text="Name of the field to evaluate"
    )
    
    field_value = models.TextField(
        null=True,
        blank=True,
        help_text="Value to compare against"
    )
    
    # Priority if multiple connections exist from the same step
    priority = models.PositiveIntegerField(default=1)
    
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        managed = False
        db_table = 'journey_step_connection'
        unique_together = ['from_step', 'to_step']
        ordering = ['from_step__order', 'priority']

    def __str__(self):
        trigger_info = ""
        if self.trigger_type == 'delay':
            trigger_info = f" (after {self.delay_duration} {self.delay_unit})"
        elif self.trigger_type == 'funnel_change':
            trigger_info = f" (on funnel step: {self.funnel_step})"
        elif self.trigger_type == 'condition':
            trigger_info = f" (if: {self.condition_label})"
            
        return f"{self.from_step.name} → {self.to_step.name}{trigger_info}"

    def clean(self):
        """Validate the connection configuration"""
        super().clean()
        
        # Same journey validation
        if self.from_step.journey != self.to_step.journey:
            raise ValidationError("Connected steps must belong to the same journey")
            
        # Prevent circular references
        if self.from_step == self.to_step:
            raise ValidationError("A step cannot connect to itself")
            
        # Validate delay settings
        if self.trigger_type == 'delay':
            if self.delay_duration is None:
                raise ValidationError("Delay duration is required for delay triggers")
            if self.delay_unit is None:
                raise ValidationError("Delay unit is required for delay triggers")
                
        # Validate funnel step settings
        if self.trigger_type == 'funnel_change' and self.funnel_step is None:
            raise ValidationError("Funnel step is required for funnel change triggers")
            
        # Validate event settings
        if self.trigger_type == 'event' and not self.event_type:
            raise ValidationError("Event type is required for event-based triggers")
            
        # Validate condition settings
        if self.trigger_type == 'condition':
            if not self.condition_type:
                raise ValidationError("Condition type is required for condition triggers")
            if not self.field_source:
                raise ValidationError("Field source is required for condition triggers")
            if not self.field_name:
                raise ValidationError("Field name is required for condition triggers")
            if self.condition_type not in ['field_is_empty', 'field_is_not_empty'] and not self.field_value:
                raise ValidationError("Field value is required for this condition type")

    def get_delay_in_seconds(self):
        """Convert the delay to seconds based on the unit"""
        if self.trigger_type != 'delay' or not self.delay_duration:
            return 0
            
        multipliers = {
            'seconds': 1,
            'minutes': 60,
            'hours': 3600,
            'days': 86400,
            'weeks': 604800
        }
        
        return self.delay_duration * multipliers.get(self.delay_unit, 1)
        
    def should_trigger(self, participant, event=None):
        """
        Determine if this connection should trigger for the given participant
        
        Args:
            participant: LeadNurturingParticipant instance
            event: Optional event data for event-based triggers
            
        Returns:
            bool: Whether the connection should trigger
        """
        # Basic check - is this the participant's current step?
        if participant.current_step != self.from_step:
            return False
            
        # Handle different trigger types
        if self.trigger_type == 'immediate':
            return True
            
        elif self.trigger_type == 'delay':
            # Check if enough time has passed since the participant entered this step
            last_entered_event = participant.events.filter(
                journey_step=self.from_step,
                event_type='enter_step'
            ).order_by('-event_timestamp').first()
            
            if not last_entered_event:
                return False
                
            delay_seconds = self.get_delay_in_seconds()
            time_passed = timezone.now() - last_entered_event.event_timestamp
            
            return time_passed.total_seconds() >= delay_seconds
            
        elif self.trigger_type == 'funnel_change':
            # Check if the participant's lead has moved to the specified funnel step
            return (
                participant.lead.current_step == self.funnel_step and 
                event and event.get('type') == 'funnel_step_changed'
            )
            
        elif self.trigger_type == 'event':
            # Check if the right event occurred
            return event and event.get('type') == self.event_type
            
        elif self.trigger_type == 'condition':
            # Evaluate the condition against the participant/lead
            return self._evaluate_condition(participant)
            
        elif self.trigger_type == 'manual':
            # Manual triggers are only activated explicitly
            return event and event.get('type') == 'manual_trigger' and event.get('connection_id') == self.id
            
        return False
        
    def _evaluate_condition(self, participant):
        """
        Evaluate the condition defined in the model fields against the participant
        
        Returns:
            bool: Whether the condition is met
        """
        if not all([self.condition_type, self.field_source, self.field_name]):
            return False
            
        # Get the lead for evaluating conditions
        lead = participant.lead
        
        # Get the field value based on the source
        field_value = self._get_field_value(lead)
        
        # Evaluate based on condition type
        if self.condition_type == 'field_equals':
            return str(field_value) == str(self.field_value)
            
        elif self.condition_type == 'field_contains':
            return str(self.field_value) in str(field_value)
            
        elif self.condition_type == 'field_greater_than':
            try:
                return float(field_value) > float(self.field_value)
            except (ValueError, TypeError):
                return False
                
        elif self.condition_type == 'field_less_than':
            try:
                return float(field_value) < float(self.field_value)
            except (ValueError, TypeError):
                return False
                
        elif self.condition_type == 'field_is_empty':
            return field_value is None or str(field_value).strip() == ''
            
        elif self.condition_type == 'field_is_not_empty':
            return field_value is not None and str(field_value).strip() != ''
            
        return False
        
    def _get_field_value(self, lead):
        """
        Get the field value based on the field source
        
        Args:
            lead: Lead instance
            
        Returns:
            The value of the field from the appropriate source
        """
        if self.field_source == 'lead':
            # Direct lead model field
            return getattr(lead, self.field_name, None)
            
        elif self.field_source == 'd2c_lead':
            # D2C Lead model field
            if hasattr(lead, 'd2c_lead'):
                return getattr(lead.d2c_lead, self.field_name, None)
            return None
            
        elif self.field_source == 'b2b_lead':
            # B2B Lead model field
            if hasattr(lead, 'b2b_lead'):
                return getattr(lead.b2b_lead, self.field_name, None)
            return None
            
        elif self.field_source == 'lead_field_value':
            # LeadFieldValue model
            if hasattr(lead, 'field_values'):
                field_value = lead.field_values.filter(
                    field_definition__api_name=self.field_name
                ).first()
                return field_value.value if field_value else None
            return None
            
        elif self.field_source == 'lead_intake_value':
            # LeadIntakeValue model
            if hasattr(lead, 'intake_values'):
                intake_value = lead.intake_values.filter(
                    field_name=self.field_name
                ).first()
                return intake_value.value if intake_value else None
            return None
            
        elif self.field_source == 'custom_field':
            # Custom field logic can be implemented here
            # This could involve looking up custom field definitions and values
            return None
            
        return None 