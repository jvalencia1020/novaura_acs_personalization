from django.core.management.base import BaseCommand
from bulkcampaign_processor.tasks.bulk_campaign_tasks import process_bulk_campaigns
import logging

logger = logging.getLogger('bulkcampaign_processor')

class Command(BaseCommand):
    help = 'Manually process all active bulk campaigns'

    def handle(self, *args, **options):
        try:
            logger.info("Starting manual bulk campaign processing")
            process_bulk_campaigns()
            logger.info("Completed manual bulk campaign processing")
            self.stdout.write(self.style.SUCCESS('Successfully processed bulk campaigns'))
        except Exception as e:
            logger.error(f"Error processing bulk campaigns: {str(e)}")
            self.stdout.write(self.style.ERROR(f'Error processing bulk campaigns: {str(e)}')) 