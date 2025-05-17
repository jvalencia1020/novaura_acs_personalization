from django.core.management.base import BaseCommand
from bulkcampaign_processor.tasks.bulk_campaign_tasks import process_due_messages
import logging

logger = logging.getLogger('bulkcampaign_processor')

class Command(BaseCommand):
    help = 'Manually process all due messages for bulk campaigns'

    def handle(self, *args, **options):
        try:
            logger.info("Starting manual due messages processing")
            process_due_messages()
            logger.info("Completed manual due messages processing")
            self.stdout.write(self.style.SUCCESS('Successfully processed due messages'))
        except Exception as e:
            logger.error(f"Error processing due messages: {str(e)}")
            self.stdout.write(self.style.ERROR(f'Error processing due messages: {str(e)}')) 