from django.apps import AppConfig


class BulkcampaignProcessorConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'bulkcampaign_processor'
    verbose_name = 'Bulk Campaign Processor'
    has_models = False
