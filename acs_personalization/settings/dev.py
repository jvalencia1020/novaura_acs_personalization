"""
Development settings for acs_personalization project.
"""

from .base import *

DEBUG = True

# Override settings for development
LOGGING['loggers']['journey_processor']['level'] = 'DEBUG'