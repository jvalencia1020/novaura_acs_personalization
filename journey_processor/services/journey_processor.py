# journey_processor/services/journey_processor.py

import logging
import json
from django.db import transaction
from django.utils import timezone
from django.db.models import Q

from acs.models import (
    Journey, JourneyStep, JourneyStepConnection,
    JourneyParticipant, JourneyEvent, LeadNurturingCampaign
)
from crm.models import Lead, FunnelStep
from journey_processor.services.condition_evaluator import ConditionEvaluator

logger = logging.getLogger(__name__)


class JourneyProcessor:
    """
    Service class for processing journey steps and transitions
    Handles journey workflow logic, participant management, and step execution
    """

    def __init__(self):
        self.condition_evaluator = ConditionEvaluator()

        # Register step processors for different step types
        self.step_processors = {
            'email': self._process_email_step,
            'sms': self._process_sms_step,
            'delay': self._process_delay_step,
            'condition': self._process_condition_step,
            'goal': self._process_goal_step,
            'webhook': self._process_webhook_step,
            'end': self._process_end_step
        }

    def process_participant(self, participant):
        """
        Process a journey participant - either start them on their journey
        or handle their current step

        Args:
            participant: JourneyParticipant instance
        """
        if not participant.is_active:
            logger.debug(f"Skipping inactive participant {participant}")
            return

        if not participant.current_step:
            # New participant - assign to an entry point
            self._assign_entry_point(participant)
        else:
            # Process the current step
            self._process_step(participant)

    def process_timed_connections(self):
        """
        Process all delay-based connections that are due to trigger
        This should be run periodically by a scheduled task

        Returns:
            int: Number of participants that were moved to next steps
        """
        logger.info("Processing timed connections...")

        # Find all active participants with a current step
        active_participants = JourneyParticipant.objects.filter(
            status='active',
            current_step__isnull=False
        ).select_related('current_step', 'journey', 'lead')

        logger.debug(f"Found {active_participants.count()} active participants")
        processed_count = 0

        for participant in active_participants:
            # Find all delay connections from the current step
            delay_connections = JourneyStepConnection.objects.filter(
                from_step=participant.current_step,
                trigger_type='delay',
                is_active=True
            ).select_related('to_step').order_by('priority')

            if not delay_connections.exists():
                continue

            logger.debug(f"Checking {delay_connections.count()} delay connections for participant {participant.id}")

            for connection in delay_connections:
                if self._should_trigger_delay(participant, connection):
                    # The delay has elapsed, move to the next step
                    logger.info(f"Triggering delay connection for participant {participant.id}: {connection}")
                    self._transition_participant(participant, connection)
                    processed_count += 1
                    # Only process the first triggering connection
                    break

        logger.info(f"Processed {processed_count} timed connections")
        return processed_count

    def process_event(self, event_type, data):
        """
        Process an external event that might trigger journey transitions

        Args:
            event_type: String identifier for the event type
            data: Dictionary with event data including lead_id/participant_id

        Returns:
            int: Number of participants that were moved to next steps
        """
        logger.info(f"Processing event {event_type} with data: {data}")

        lead_id = data.get('lead_id')
        participant_id = data.get('participant_id')

        # Find affected participants
        participants = []

        if participant_id:
            try:
                participants = [JourneyParticipant.objects.get(
                    id=participant_id,
                    status='active'
                )]
            except JourneyParticipant.DoesNotExist:
                logger.warning(f"Participant {participant_id} not found")
                return 0

        elif lead_id:
            try:
                participants = JourneyParticipant.objects.filter(
                    lead_id=lead_id,
                    status='active'
                ).select_related('current_step', 'journey', 'lead')
            except Exception as e:
                logger.warning(f"Error finding participants for lead {lead_id}: {e}")
                return 0

        if not participants:
            logger.warning("No participants found for event")
            return 0

        logger.debug(f"Found {len(participants)} participants for event")

        processed_count = 0

        # Create event object for passing to connections
        event_obj = {
            'type': event_type,
            'data': data,
            'timestamp': timezone.now()
        }

        # Check each participant for event-triggered connections
        for participant in participants:
            if not participant.current_step:
                continue

            # Find connections that might be triggered by this event
            event_connections = self._get_event_connections(participant, event_type, data)

            for connection in event_connections:
                if self._should_trigger_event(participant, connection, event_obj):
                    logger.info(f"Triggering event connection for participant {participant.id}: {connection}")
                    self._transition_participant(participant, connection, event_obj)
                    processed_count += 1
                    # Only process the first triggered connection
                    break

        logger.info(f"Processed event {event_type} for {processed_count} participants")
        return processed_count

    def _assign_entry_point(self, participant):
        """Assign a participant to an entry point in the journey"""
        entry_points = participant.journey.steps.filter(
            is_entry_point=True,
            is_active=True
        ).order_by('order')

        if not entry_points.exists():
            logger.error(f"No active entry points found for journey {participant.journey}")
            return

        # Start with the first entry point (assuming order is respected)
        entry_point = entry_points.first()

        logger.info(f"Assigning participant {participant.id} to entry point {entry_point.name}")

        # Move to the entry point
        participant.move_to_next_step(entry_point, 'enter_journey', {
            'entry_type': 'initial',
            'journey_id': str(participant.journey.id)
        })

        # Process the entry point step
        self._process_step(participant)

    def _process_step(self, participant):
        """Process a participant's current step"""
        current_step = participant.current_step

        if not current_step or not current_step.is_active:
            logger.warning(f"Cannot process inactive or null step for {participant.id}")
            return

        # Get the step processor for this step type
        step_processor = self.step_processors.get(current_step.step_type)

        if not step_processor:
            logger.error(f"No processor found for step type: {current_step.step_type}")
            return

        # Process the step
        logger.debug(
            f"Processing step {current_step.name} of type {current_step.step_type} for participant {participant.id}")
        result = step_processor(participant, current_step)

        # If the step processing indicates immediate transition, handle it
        if result.get('transition_immediately', False):
            # Find the next connection based on priority
            connections = current_step.next_connections.filter(
                trigger_type='immediate',
                is_active=True
            ).order_by('priority')

            if connections.exists():
                logger.debug(f"Immediate transition from {current_step.name} for participant {participant.id}")
                self._transition_participant(participant, connections.first())

    def _transition_participant(self, participant, connection, event=None):
        """
        Move a participant from one step to another

        Args:
            participant: JourneyParticipant instance
            connection: JourneyStepConnection instance
            event: Optional event data that triggered this transition
        """
        from_step = connection.from_step
        to_step = connection.to_step

        logger.info(f"Transitioning {participant.id} from {from_step.name} to {to_step.name}")

        try:
            with transaction.atomic():
                # Record exit from current step
                self._create_event(participant, from_step, 'exit_step', {
                    'connection_id': str(connection.id),
                    'connection_type': connection.trigger_type,
                    'event_data': event
                })

                # Move the participant to the next step
                participant.move_to_next_step(to_step, 'enter_step', {
                    'from_step_id': str(from_step.id),
                    'connection_id': str(connection.id)
                })

                # Process the new step immediately if needed
                if to_step.step_type in ['condition', 'webhook', 'end']:
                    self._process_step(participant)
        except Exception as e:
            logger.exception(f"Error transitioning participant {participant.id}: {e}")

    def _get_event_connections(self, participant, event_type, data):
        """Get connections that might be triggered by this event"""
        connections = []

        # Event connections
        if event_type:
            event_connections = participant.current_step.next_connections.filter(
                trigger_type='event',
                event_type=event_type,
                is_active=True
            ).order_by('priority')
            connections.extend(event_connections)

        # Funnel step change connections
        if event_type == 'funnel_step_changed' and 'funnel_step_id' in data:
            funnel_step_id = data.get('funnel_step_id')
            if funnel_step_id:
                funnel_connections = participant.current_step.next_connections.filter(
                    trigger_type='funnel_change',
                    funnel_step_id=funnel_step_id,
                    is_active=True
                ).order_by('priority')
                connections.extend(funnel_connections)

        # Manual trigger connections
        if event_type == 'manual_trigger' and 'connection_id' in data:
            connection_id = data.get('connection_id')
            if connection_id:
                manual_connections = participant.current_step.next_connections.filter(
                    id=connection_id,
                    trigger_type='manual',
                    is_active=True
                )
                connections.extend(manual_connections)

        return connections

    def _should_trigger_delay(self, participant, connection):
        """
        Check if a delay connection should trigger for participant

        Args:
            participant: JourneyParticipant instance
            connection: JourneyStepConnection instance with trigger_type='delay'

        Returns:
            bool: Whether the delay has passed and connection should trigger
        """
        # Find the last time the participant entered this step
        last_entry_event = participant.events.filter(
            journey_step=participant.current_step,
            event_type='enter_step'
        ).order_by('-event_timestamp').first()

        if not last_entry_event:
            logger.warning(
                f"No enter_step event found for participant {participant.id} at step {participant.current_step.name}")
            return False

        # Calculate the delay
        delay_seconds = self._get_delay_in_seconds(connection)
        if delay_seconds <= 0:
            logger.warning(f"Invalid delay duration: {delay_seconds} seconds")
            return False

        # Check if enough time has passed
        time_elapsed = timezone.now() - last_entry_event.event_timestamp
        return time_elapsed.total_seconds() >= delay_seconds

    def _should_trigger_event(self, participant, connection, event):
        """
        Check if an event-based connection should trigger for participant

        Args:
            participant: JourneyParticipant instance
            connection: JourneyStepConnection instance
            event: Event data dict

        Returns:
            bool: Whether the connection should trigger
        """
        event_type = event.get('type')
        event_data = event.get('data', {})

        # Basic checks
        if participant.current_step_id != connection.from_step_id:
            return False

        # Event type connections
        if connection.trigger_type == 'event' and connection.event_type == event_type:
            return True

        # Funnel step change connections
        if (connection.trigger_type == 'funnel_change' and
                event_type == 'funnel_step_changed' and
                connection.funnel_step_id == event_data.get('funnel_step_id')):
            return True

        # Manual trigger connections
        if (connection.trigger_type == 'manual' and
                event_type == 'manual_trigger' and
                connection.id == event_data.get('connection_id')):
            return True

        return False

    def _get_delay_in_seconds(self, connection):
        """Convert a delay connection's duration to seconds"""
        if not connection.delay_duration:
            return 0

        # Multiplication factors for different time units
        unit_factors = {
            'seconds': 1,
            'minutes': 60,
            'hours': 3600,
            'days': 86400,
            'weeks': 604800
        }

        unit = connection.delay_unit or 'seconds'
        factor = unit_factors.get(unit, 1)

        return connection.delay_duration * factor

    def _create_event(self, participant, step, event_type, metadata=None):
        """
        Create a journey event record

        Args:
            participant: JourneyParticipant instance
            step: JourneyStep instance
            event_type: String event type
            metadata: Optional JSON-serializable metadata
        """
        try:
            event = JourneyEvent.objects.create(
                participant=participant,
                journey_step=step,
                event_type=event_type,
                metadata=metadata,
                created_by=participant.last_updated_by
            )
            logger.debug(f"Created event {event_type} for participant {participant.id} at step {step.name}")
            return event
        except Exception as e:
            logger.exception(f"Error creating journey event: {e}")
            return None

    # Step processors for different step types

    def _process_email_step(self, participant, step):
        """
        Process an email step

        Returns a result dict with:
            success: Whether the email was sent successfully
            transition_immediately: Whether to transition immediately to next step
        """
        if not step.template:
            logger.error(f"Email step {step.name} has no template")
            self._create_event(participant, step, 'error', {
                'error': 'No template configured for email step'
            })
            return {'success': False, 'transition_immediately': False}

        # Get the lead's email
        lead = participant.lead
        if not lead.email:
            logger.warning(f"Cannot send email to lead {lead.id} with no email address")
            self._create_event(participant, step, 'error', {
                'error': 'Lead has no email address'
            })
            return {'success': False, 'transition_immediately': True}

        try:
            # Create the event first (would be updated with tracking info)
            event = self._create_event(participant, step, 'action_sent', {
                'action_type': 'email',
                'template_id': str(step.template.id),
                'recipient': lead.email
            })

            # This is where you would integrate with your email service
            # For example:
            # email_service.send_email(
            #     to_email=lead.email,
            #     template_id=step.template.id,
            #     context={
            #         'lead': lead,
            #         'participant': participant,
            #         'journey': participant.journey,
            #         'tracking_id': str(event.id) if event else None
            #     }
            # )

            # For now, we'll just log the email sending
            logger.info(f"Would send email to {lead.email} using template {step.template.id}")

            return {'success': True, 'transition_immediately': True}

        except Exception as e:
            logger.exception(f"Error sending email for {participant.id} at {step.name}: {e}")
            self._create_event(participant, step, 'error', {
                'error': str(e),
                'action_type': 'email'
            })
            return {'success': False, 'transition_immediately': True}

    def _process_sms_step(self, participant, step):
        """
        Process an SMS step

        Returns a result dict with:
            success: Whether the SMS was sent successfully
            transition_immediately: Whether to transition immediately to next step
        """
        if not step.template:
            logger.error(f"SMS step {step.name} has no template")
            self._create_event(participant, step, 'error', {
                'error': 'No template configured for SMS step'
            })
            return {'success': False, 'transition_immediately': False}

        # Get the lead's phone number
        lead = participant.lead
        if not lead.phone_number:
            logger.warning(f"Cannot send SMS to lead {lead.id} with no phone number")
            self._create_event(participant, step, 'error', {
                'error': 'Lead has no phone number'
            })
            return {'success': False, 'transition_immediately': True}

        try:
            # Create the event first (would be updated with tracking info)
            event = self._create_event(participant, step, 'action_sent', {
                'action_type': 'sms',
                'template_id': str(step.template.id),
                'recipient': lead.phone_number
            })

            # This is where you would integrate with your SMS service
            # For example:
            # sms_service.send_sms(
            #     to_phone=lead.phone_number,
            #     template_id=step.template.id,
            #     context={
            #         'lead': lead,
            #         'participant': participant,
            #         'journey': participant.journey,
            #         'tracking_id': str(event.id) if event else None
            #     }
            # )

            # For now, we'll just log the SMS sending
            logger.info(f"Would send SMS to {lead.phone_number} using template {step.template.id}")

            return {'success': True, 'transition_immediately': True}

        except Exception as e:
            logger.exception(f"Error sending SMS for {participant.id} at {step.name}: {e}")
            self._create_event(participant, step, 'error', {
                'error': str(e),
                'action_type': 'sms'
            })
            return {'success': False, 'transition_immediately': True}

    def _process_delay_step(self, participant, step):
        """
        Process a delay step - these are just waiting points
        The actual delay is handled by the delay connection

        Returns a result dict with:
            success: Always true
            transition_immediately: False (delays don't transition immediately)
        """
        # Delay steps don't do anything on their own - they're just waiting points
        # Record that we're entering a delay
        self._create_event(participant, step, 'delay_started', {
            'delay_config': step.config
        })

        return {'success': True, 'transition_immediately': False}

    def _process_condition_step(self, participant, step):
        """
        Process a condition step

        Returns a result dict with:
            success: Whether the condition evaluation succeeded
            transition_immediately: False (condition transitions via specific paths)
        """
        config = step.config or {}
        lead = participant.lead

        try:
            # Evaluate the condition using the condition evaluator
            condition_met = self.condition_evaluator.evaluate(lead, config)

            # Record the condition result
            self._create_event(
                participant,
                step,
                'condition_met' if condition_met else 'condition_not_met',
                {
                    'condition': config,
                    'result': condition_met
                }
            )

            # Find the right connection based on condition result
            if condition_met:
                # Find the "true" path
                connections = step.next_connections.filter(
                    Q(condition_label__iexact='true') |
                    Q(condition_label__iexact='yes'),
                    is_active=True
                ).order_by('priority')
            else:
                # Find the "false" path
                connections = step.next_connections.filter(
                    Q(condition_label__iexact='false') |
                    Q(condition_label__iexact='no'),
                    is_active=True
                ).order_by('priority')

            if connections.exists():
                self._transition_participant(participant, connections.first())
                return {'success': True, 'transition_immediately': False}  # Already transitioned

            # If no matching connection, try a default one
            default_connections = step.next_connections.filter(
                Q(condition_label__isnull=True) |
                Q(condition_label__exact=''),
                is_active=True
            ).order_by('priority')

            if default_connections.exists():
                self._transition_participant(participant, default_connections.first())
                return {'success': True, 'transition_immediately': False}  # Already transitioned

            # If still no connections, log a warning
            logger.warning(f"Condition step {step.name} has no valid next connections for result: {condition_met}")
            return {'success': False, 'transition_immediately': False}

        except Exception as e:
            logger.exception(f"Error processing condition for {participant.id} at {step.name}: {e}")
            self._create_event(participant, step, 'error', {
                'error': str(e),
                'condition': config
            })
            return {'success': False, 'transition_immediately': True}

    def _process_goal_step(self, participant, step):
        """
        Process a goal step - these represent achievement of a goal

        Returns a result dict with:
            success: Always true
            transition_immediately: True (goals always transition to next step)
        """
        # Record goal achievement
        self._create_event(participant, step, 'goal_achieved', {
            'goal_type': step.config.get('goal_type'),
            'goal_value': step.config.get('goal_value')
        })

        # Goals automatically transition to the next step
        return {'success': True, 'transition_immediately': True}

    def _process_webhook_step(self, participant, step):
        """
        Process a webhook step - calls an external API

        Returns a result dict with:
            success: Whether the webhook call succeeded
            transition_immediately: True (webhooks transition after completion)
        """
        webhook_config = step.config or {}
        webhook_url = webhook_config.get('url')
        method = webhook_config.get('method', 'POST')

        if not webhook_url:
            logger.error(f"Webhook step {step.name} has no URL configured")
            self._create_event(participant, step, 'error', {
                'error': 'No webhook URL configured'
            })
            return {'success': False, 'transition_immediately': True}

        try:
            # Prepare data for the webhook
            lead = participant.lead
            webhook_data = {
                'participant_id': str(participant.id),
                'lead_id': str(lead.id),
                'journey_id': str(participant.journey.id),
                'step_id': str(step.id),
                'timestamp': timezone.now().isoformat(),
                'lead_data': {
                    'first_name': lead.first_name,
                    'last_name': lead.last_name,
                    'email': lead.email,
                    'phone_number': lead.phone_number,
                },
                'custom_data': webhook_config.get('custom_data', {})
            }

            # This would call your actual webhook service
            # For now, we'll just log the webhook call
            logger.info(f"Would call webhook at {webhook_url} with method {method}")

            # In a real implementation:
            # response = self._call_webhook(webhook_url, method, webhook_data)

            # Record the webhook call
            self._create_event(participant, step, 'action_sent', {
                'action_type': 'webhook',
                'webhook_url': webhook_url,
                'webhook_method': method,
                'webhook_data': webhook_data,
                # 'webhook_response': response
            })

            return {'success': True, 'transition_immediately': True}

        except Exception as e:
            logger.exception(f"Error calling webhook for {participant.id} at {step.name}: {e}")
            self._create_event(participant, step, 'error', {
                'error': str(e),
                'action_type': 'webhook'
            })
            return {'success': False, 'transition_immediately': True}

    def _process_end_step(self, participant, step):
        """
        Process an end step - completes the journey for this participant

        Returns a result dict with:
            success: Always true
            transition_immediately: False (end steps don't transition)
        """
        # Mark the participant as completed
        participant.status = 'completed'
        participant.save(update_fields=['status', 'updated_at'])

        # Record the journey completion
        self._create_event(participant, step, 'exit_journey', {
            'completion_type': 'normal',
            'journey_id': str(participant.journey.id),
            'duration_seconds': (timezone.now() - participant.entered_at).total_seconds()
        })

        logger.info(f"Participant {participant.id} completed journey {participant.journey.name}")
        return {'success': True, 'transition_immediately': False}

    # Utility methods for external services

    def _call_webhook(self, url, method, data):
        """
        Call a webhook URL

        Args:
            url: Webhook URL
            method: HTTP method (GET, POST, etc.)
            data: Data to send

        Returns:
            Response text
        """
        import requests

        headers = {
            'Content-Type': 'application/json',
            'User-Agent': 'Novaura-Journey-Processor/1.0'
        }

        if method.upper() == 'GET':
            response = requests.get(
                url,
                params=data,
                headers=headers,
                timeout=10  # 10 second timeout
            )
        else:
            response = requests.post(
                url,
                json=data,
                headers=headers,
                timeout=10  # 10 second timeout
            )

        response.raise_for_status()
        return response.text