# journey_processor/services/condition_evaluator.py

import logging
import json
from django.db.models import Q
from django.utils import timezone

from external_models.models.journeys import JourneyStepConnection
from external_models.models.nurturing_campaigns import LeadNurturingParticipant
from crm.models import Lead, FunnelStep

logger = logging.getLogger(__name__)


class ConditionEvaluator:
    """
    Service class for evaluating journey step conditions
    Handles validation of conditions for step transitions
    """

    def evaluate(self, connection, participant, event_data=None):
        """
        Evaluate if a connection's conditions are met for a participant

        Args:
            connection: JourneyStepConnection instance
            participant: LeadNurturingParticipant instance
            event_data: Optional dictionary with event data

        Returns:
            bool: True if conditions are met, False otherwise
        """
        if not connection.conditions:
            return True

        try:
            conditions = json.loads(connection.conditions)
        except json.JSONDecodeError:
            logger.error(f"Invalid conditions JSON for connection {connection.id}")
            return False

        if not conditions:
            return True

        # Evaluate each condition group (AND logic between groups)
        for group in conditions:
            if not self._evaluate_group(group, participant, event_data):
                return False

        return True

    def _evaluate_group(self, group, participant, event_data):
        """
        Evaluate a group of conditions (OR logic within group)

        Args:
            group: List of condition dictionaries
            participant: LeadNurturingParticipant instance
            event_data: Optional dictionary with event data

        Returns:
            bool: True if any condition in group is met, False otherwise
        """
        if not group:
            return True

        # Evaluate each condition in the group (OR logic)
        for condition in group:
            if self._evaluate_condition(condition, participant, event_data):
                return True

        return False

    def _evaluate_condition(self, condition, participant, event_data):
        """
        Evaluate a single condition

        Args:
            condition: Dictionary with condition details
            participant: LeadNurturingParticipant instance
            event_data: Optional dictionary with event data

        Returns:
            bool: True if condition is met, False otherwise
        """
        condition_type = condition.get('type')
        if not condition_type:
            return False

        # Get the appropriate evaluator method
        evaluator = getattr(self, f'_evaluate_{condition_type}', None)
        if not evaluator:
            logger.warning(f"Unknown condition type: {condition_type}")
            return False

        try:
            return evaluator(condition, participant, event_data)
        except Exception as e:
            logger.exception(f"Error evaluating condition: {e}")
            return False

    def _evaluate_funnel_step(self, condition, participant, event_data):
        """Evaluate if lead is in specified funnel step"""
        funnel_step_id = condition.get('funnel_step_id')
        if not funnel_step_id:
            return False

        try:
            return participant.lead.funnel_step_id == funnel_step_id
        except Exception as e:
            logger.exception(f"Error checking funnel step: {e}")
            return False

    def _evaluate_lead_status(self, condition, participant, event_data):
        """Evaluate lead status condition"""
        status = condition.get('status')
        if not status:
            return False

        try:
            return participant.lead.status == status
        except Exception as e:
            logger.exception(f"Error checking lead status: {e}")
            return False

    def _evaluate_lead_score(self, condition, participant, event_data):
        """Evaluate lead score condition"""
        operator = condition.get('operator')
        value = condition.get('value')

        if not operator or value is None:
            return False

        try:
            lead_score = participant.lead.score or 0
            if operator == 'gt':
                return lead_score > value
            elif operator == 'gte':
                return lead_score >= value
            elif operator == 'lt':
                return lead_score < value
            elif operator == 'lte':
                return lead_score <= value
            elif operator == 'eq':
                return lead_score == value
            else:
                logger.warning(f"Unknown operator: {operator}")
                return False
        except Exception as e:
            logger.exception(f"Error checking lead score: {e}")
            return False

    def _evaluate_lead_property(self, condition, participant, event_data):
        """Evaluate lead property condition"""
        property_name = condition.get('property')
        operator = condition.get('operator')
        value = condition.get('value')

        if not property_name or not operator or value is None:
            return False

        try:
            # Get property value from lead
            property_value = getattr(participant.lead, property_name, None)
            if property_value is None:
                return False

            # Compare values based on operator
            if operator == 'eq':
                return property_value == value
            elif operator == 'neq':
                return property_value != value
            elif operator == 'contains':
                return str(value) in str(property_value)
            elif operator == 'not_contains':
                return str(value) not in str(property_value)
            else:
                logger.warning(f"Unknown operator: {operator}")
                return False
        except Exception as e:
            logger.exception(f"Error checking lead property: {e}")
            return False

    def _evaluate_event_property(self, condition, participant, event_data):
        """Evaluate event property condition"""
        if not event_data:
            return False

        property_name = condition.get('property')
        operator = condition.get('operator')
        value = condition.get('value')

        if not property_name or not operator or value is None:
            return False

        try:
            # Get property value from event data
            property_value = event_data.get(property_name)
            if property_value is None:
                return False

            # Compare values based on operator
            if operator == 'eq':
                return property_value == value
            elif operator == 'neq':
                return property_value != value
            elif operator == 'contains':
                return str(value) in str(property_value)
            elif operator == 'not_contains':
                return str(value) not in str(property_value)
            else:
                logger.warning(f"Unknown operator: {operator}")
                return False
        except Exception as e:
            logger.exception(f"Error checking event property: {e}")
            return False

    def _evaluate_time_elapsed(self, condition, participant, event_data):
        """Evaluate time elapsed condition"""
        step_id = condition.get('step_id')
        operator = condition.get('operator')
        value = condition.get('value')

        if not step_id or not operator or value is None:
            return False

        try:
            # Get the last enter_step event for the specified step
            last_enter = participant.events.filter(
                journey_step_id=step_id,
                event_type='enter_step'
            ).order_by('-event_timestamp').first()

            if not last_enter:
                return False

            # Calculate elapsed time in seconds
            elapsed = timezone.now() - last_enter.event_timestamp
            elapsed_seconds = elapsed.total_seconds()

            # Compare elapsed time based on operator
            if operator == 'gt':
                return elapsed_seconds > value
            elif operator == 'gte':
                return elapsed_seconds >= value
            elif operator == 'lt':
                return elapsed_seconds < value
            elif operator == 'lte':
                return elapsed_seconds <= value
            else:
                logger.warning(f"Unknown operator: {operator}")
                return False
        except Exception as e:
            logger.exception(f"Error checking time elapsed: {e}")
            return False

    def _evaluate_step_count(self, condition, participant, event_data):
        """Evaluate step count condition"""
        step_type = condition.get('step_type')
        operator = condition.get('operator')
        value = condition.get('value')

        if not step_type or not operator or value is None:
            return False

        try:
            # Count events of specified step type
            count = participant.events.filter(
                journey_step__step_type=step_type,
                event_type='enter_step'
            ).count()

            # Compare count based on operator
            if operator == 'gt':
                return count > value
            elif operator == 'gte':
                return count >= value
            elif operator == 'lt':
                return count < value
            elif operator == 'lte':
                return count <= value
            elif operator == 'eq':
                return count == value
            else:
                logger.warning(f"Unknown operator: {operator}")
                return False
        except Exception as e:
            logger.exception(f"Error checking step count: {e}")
            return False