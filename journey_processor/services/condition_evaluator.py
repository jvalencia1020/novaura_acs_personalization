# journey_processor/services/condition_evaluator.py

import logging
import re
import operator
from datetime import timedelta
from django.utils import timezone
from django.db.models import Q

logger = logging.getLogger(__name__)


class ConditionEvaluator:
    """
    Evaluates conditions for journey branching logic
    This handles complex condition evaluation against lead properties
    and related model data
    """

    # Map of operator names to functions
    OPERATORS = {
        'eq': operator.eq,
        'neq': operator.ne,
        'gt': operator.gt,
        'lt': operator.lt,
        'gte': operator.ge,
        'lte': operator.le,
        'contains': lambda a, b: b in a if a is not None and isinstance(a, (str, list)) else False,
        'not_contains': lambda a, b: b not in a if a is not None and isinstance(a, (str, list)) else True,
        'starts_with': lambda a, b: a.startswith(b) if a is not None and isinstance(a, str) else False,
        'ends_with': lambda a, b: a.endswith(b) if a is not None and isinstance(a, str) else False,
        'matches': lambda a, b: bool(re.match(b, a)) if a is not None and isinstance(a, str) else False,
        'is_empty': lambda a, b: a is None or a == '',
        'is_not_empty': lambda a, b: a is not None and a != '',
        'in_past': lambda a, b: a < timezone.now() if a is not None else False,
        'in_future': lambda a, b: a > timezone.now() if a is not None else False,
        'days_ago': lambda a, b: (timezone.now() - a).days > int(b) if a is not None else False,
        'within_days': lambda a, b: (timezone.now() - a).days <= int(b) if a is not None else False,
        'in_list': lambda a, b: a in b if isinstance(b, (list, tuple)) else False,
        'not_in_list': lambda a, b: a not in b if isinstance(b, (list, tuple)) else True,
    }

    def evaluate(self, lead, condition_config):
        """
        Evaluate a condition against a lead

        Args:
            lead: Lead model instance
            condition_config: Dict with condition configuration:
                {
                    'type': 'field_condition',  # or 'combined_condition'
                    'field': 'field_name' or 'model.field_name',
                    'operator': 'eq',  # One of OPERATORS keys
                    'value': comparison value
                }

        Returns:
            bool: Whether the condition is met
        """
        try:
            condition_type = condition_config.get('type', 'field_condition')

            if condition_type == 'field_condition':
                return self._evaluate_field_condition(lead, condition_config)
            elif condition_type == 'combined_condition':
                return self._evaluate_combined_condition(lead, condition_config)
            else:
                logger.warning(f"Unknown condition type: {condition_type}")
                return False

        except Exception as e:
            logger.exception(f"Error evaluating condition: {e}")
            return False

    def _evaluate_field_condition(self, lead, condition_config):
        """
        Evaluate a condition based on field value

        Args:
            lead: Lead model instance
            condition_config: Condition configuration dict

        Returns:
            bool: Whether the condition is met
        """
        field = condition_config.get('field')
        if not field:
            logger.warning("Missing field in condition configuration")
            return False

        op_name = condition_config.get('operator', 'eq')
        compare_value = condition_config.get('value')

        # Get the current value from the lead or related models
        current_value = self._get_field_value(lead, field)

        # Get the operator function
        op_func = self.OPERATORS.get(op_name)
        if not op_func:
            logger.warning(f"Unknown operator: {op_name}")
            return False

        # Type conversion for numeric comparisons if needed
        if op_name in ('gt', 'lt', 'gte', 'lte') and current_value is not None:
            try:
                if isinstance(compare_value, str) and compare_value.isdigit():
                    compare_value = int(compare_value)
                if isinstance(current_value, str) and current_value.isdigit():
                    current_value = int(current_value)
            except (ValueError, TypeError):
                pass

        # Compare the values
        try:
            result = op_func(current_value, compare_value)
            logger.debug(f"Condition '{field} {op_name} {compare_value}': {result} (current value: {current_value})")
            return result
        except (TypeError, ValueError) as e:
            logger.warning(f"Error comparing values ({current_value} {op_name} {compare_value}): {e}")
            return False

    def _evaluate_combined_condition(self, lead, condition_config):
        """
        Evaluate a combination of conditions with AND/OR logic

        Args:
            lead: Lead model instance
            condition_config: Dict with combined conditions configuration:
                {
                    'type': 'combined_condition',
                    'operator': 'and' or 'or',
                    'conditions': [condition1, condition2, ...]
                }

        Returns:
            bool: Whether the combined condition is met
        """
        operator_type = condition_config.get('operator', 'and').lower()
        conditions = condition_config.get('conditions', [])

        if not conditions:
            logger.warning("No conditions in combined_condition")
            return False

        if operator_type == 'and':
            for condition in conditions:
                if not self.evaluate(lead, condition):
                    return False
            return True
        elif operator_type == 'or':
            for condition in conditions:
                if self.evaluate(lead, condition):
                    return True
            return False
        else:
            logger.warning(f"Unknown combination operator: {operator_type}")
            return False

    def _get_field_value(self, lead, field):
        """
        Get a field value from the lead or related models
        Supports dot notation for related models and custom fields

        Args:
            lead: Lead model instance
            field: Field name, which can include dots for related models
                   e.g., 'first_name', 'funnel_step.name', 'custom.field_name'

        Returns:
            The field value or None if not found
        """
        # Handle special field types
        if field.startswith('custom.'):
            # Custom field access through the field_values model
            custom_field_name = field.replace('custom.', '')
            try:
                field_value = lead.field_values.filter(
                    field_definition__api_name=custom_field_name
                ).first()
                return field_value.value if field_value else None
            except Exception as e:
                logger.warning(f"Error accessing custom field {custom_field_name}: {e}")
                return None

        # Check for relationship traversal
        if '.' in field:
            parts = field.split('.')
            obj = lead

            for part in parts[:-1]:
                if not hasattr(obj, part):
                    return None

                obj = getattr(obj, part)
                if obj is None:
                    return None

            last_part = parts[-1]
            return getattr(obj, last_part, None)

        # Direct attribute access
        return getattr(lead, field, None)