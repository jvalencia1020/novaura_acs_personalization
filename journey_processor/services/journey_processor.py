# journey_processor/services/journey_processor.py

import logging
import json
from django.db import transaction
from django.utils import timezone
from django.db.models import Q

from external_models.models.journeys import Journey, JourneyStep, JourneyStepConnection, JourneyEvent
from external_models.models.nurturing_campaigns import LeadNurturingParticipant
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
            'voice': self._process_voice_step,
            'chat': self._process_chat_step,
            'wait_step': self._process_wait_step,
            'validation_step': self._process_validation_step,
            'goal': self._process_goal_step,
            'webhook': self._process_webhook_step,
            'end': self._process_end_step
        }

    def process_participant(self, participant):
        """
        Process a journey participant - either start them on their journey
        or handle their current step

        Args:
            participant: LeadNurturingParticipant instance
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
        active_participants = LeadNurturingParticipant.objects.filter(
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
                participants = [LeadNurturingParticipant.objects.get(
                    id=participant_id,
                    status='active'
                )]
            except LeadNurturingParticipant.DoesNotExist:
                logger.warning(f"Participant {participant_id} not found")
                return 0

        elif lead_id:
            try:
                participants = LeadNurturingParticipant.objects.filter(
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
        participant.current_step = entry_point
        participant.save()

        # Create entry event
        self._create_event(participant, entry_point, 'enter_step', {
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
            # Find immediate connections
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
            participant: LeadNurturingParticipant instance
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

                # Update participant's current step
                participant.current_step = to_step
                participant.save()

                # Record entry to new step
                self._create_event(participant, to_step, 'enter_step', {
                    'previous_step_id': str(from_step.id),
                    'connection_id': str(connection.id),
                    'trigger_event': event.get('type') if event else None
                })

                # Process the new step
                self._process_step(participant)

        except Exception as e:
            logger.exception(f"Error transitioning participant {participant.id}: {e}")
            raise

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
        """Check if a delay connection should trigger for participant"""
        if connection.trigger_type != 'delay':
            return False

        # Get the last enter_step event for this step
        last_enter = JourneyEvent.objects.filter(
            participant=participant,
            journey_step=connection.from_step,
            event_type='enter_step'
        ).order_by('-event_timestamp').first()

        if not last_enter:
            return False

        # Check if delay duration has elapsed
        delay_seconds = connection.get_delay_in_seconds()
        if not delay_seconds:
            return False

        elapsed = timezone.now() - last_enter.event_timestamp
        return elapsed.total_seconds() >= delay_seconds

    def _should_trigger_event(self, participant, connection, event):
        """Check if an event-based connection should trigger for participant"""
        if connection.trigger_type == 'condition':
            return connection.should_trigger(participant, event)
        return True

    def _create_event(self, participant, step, event_type, metadata=None):
        """Create a journey event record"""
        return JourneyEvent.objects.create(
            participant=participant,
            journey_step=step,
            event_type=event_type,
            metadata=metadata or {}
        )

    def _process_email_step(self, participant, step):
        """Process an email step"""
        template = step.template
        if not template:
            logger.error(f"Email step {step.id} has no template")
            return {'success': False}

        # Send email using template
        # Implementation depends on your email sending service
        try:
            # TODO: Implement email sending
            self._create_event(participant, step, 'action_sent', {
                'template_id': str(template.id)
            })
            return {'success': True, 'transition_immediately': True}
        except Exception as e:
            logger.exception(f"Error sending email for step {step.id}: {e}")
            return {'success': False}

    def _process_sms_step(self, participant, step):
        """Process an SMS step"""
        template = step.template
        if not template:
            logger.error(f"SMS step {step.id} has no template")
            return {'success': False}

        # Send SMS using template
        # Implementation depends on your SMS sending service
        try:
            # TODO: Implement SMS sending
            self._create_event(participant, step, 'action_sent', {
                'template_id': str(template.id)
            })
            return {'success': True, 'transition_immediately': True}
        except Exception as e:
            logger.exception(f"Error sending SMS for step {step.id}: {e}")
            return {'success': False}

    def _process_voice_step(self, participant, step):
        """Process a voice call step"""
        template = step.template
        if not template:
            logger.error(f"Voice step {step.id} has no template")
            return {'success': False}

        # Make voice call using template
        # Implementation depends on your voice calling service
        try:
            # TODO: Implement voice call
            self._create_event(participant, step, 'action_sent', {
                'template_id': str(template.id)
            })
            return {'success': True, 'transition_immediately': True}
        except Exception as e:
            logger.exception(f"Error making voice call for step {step.id}: {e}")
            return {'success': False}

    def _process_chat_step(self, participant, step):
        """Process a chat message step"""
        template = step.template
        if not template:
            logger.error(f"Chat step {step.id} has no template")
            return {'success': False}

        # Send chat message using template
        # Implementation depends on your chat service
        try:
            # TODO: Implement chat message sending
            self._create_event(participant, step, 'action_sent', {
                'template_id': str(template.id)
            })
            return {'success': True, 'transition_immediately': True}
        except Exception as e:
            logger.exception(f"Error sending chat message for step {step.id}: {e}")
            return {'success': False}

    def _process_wait_step(self, participant, step):
        """Process a wait step"""
        duration = step.config.get('duration')
        if not duration:
            logger.error(f"Wait step {step.id} has no duration configured")
            return {'success': False}

        # Create wait event
        self._create_event(participant, step, 'enter_step', {
            'duration': duration
        })

        return {'success': True}

    def _process_validation_step(self, participant, step):
        """Process a validation step"""
        validation_type = step.config.get('validation_type')
        if not validation_type:
            logger.error(f"Validation step {step.id} has no validation type configured")
            return {'success': False}

        # Create validation event
        self._create_event(participant, step, 'enter_step', {
            'validation_type': validation_type
        })

        return {'success': True}

    def _process_goal_step(self, participant, step):
        """Process a goal step"""
        # Create goal event
        self._create_event(participant, step, 'enter_step', {
            'goal_type': step.config.get('goal_type', 'default')
        })

        return {'success': True, 'transition_immediately': True}

    def _process_webhook_step(self, participant, step):
        """Process a webhook step"""
        url = step.config.get('url')
        if not url:
            logger.error(f"Webhook step {step.id} has no URL configured")
            return {'success': False}

        method = step.config.get('method', 'POST')
        headers = step.config.get('headers', {})
        payload = step.config.get('payload', {})

        try:
            # Call webhook
            response = self._call_webhook(url, method, headers, payload)
            
            # Create webhook event
            self._create_event(participant, step, 'action_sent', {
                'url': url,
                'method': method,
                'response_status': response.status_code
            })

            return {'success': True, 'transition_immediately': True}
        except Exception as e:
            logger.exception(f"Error calling webhook for step {step.id}: {e}")
            return {'success': False}

    def _process_end_step(self, participant, step):
        """Process an end step"""
        # Create end event
        self._create_event(participant, step, 'enter_step', {
            'end_type': step.config.get('end_type', 'default')
        })

        # Mark participant as completed
        participant.status = 'completed'
        participant.save()

        return {'success': True}

    def _call_webhook(self, url, method, headers, data):
        """Make an HTTP request to a webhook URL"""
        import requests

        try:
            response = requests.request(
                method=method,
                url=url,
                headers=headers,
                json=data,
                timeout=30
            )
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            logger.exception(f"Webhook request failed: {e}")
            raise