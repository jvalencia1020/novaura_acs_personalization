from django.apps import AppConfig


class JourneyProcessorConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'journey_processor'
    verbose_name = 'Journey Processor'
    has_models = False
