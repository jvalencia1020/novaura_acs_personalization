"""
Production settings for acs_personalization project.
"""

from .base import *
import os

# No DEBUG in production
DEBUG = False

# Define allowed hosts for production
ALLOWED_HOSTS = [
    os.getenv('ALLOWED_HOSTS', '*').split(',')  # Get from environment variable or default to '*'
]

# Additional production-specific settings
# AWS-specific settings would go here if needed