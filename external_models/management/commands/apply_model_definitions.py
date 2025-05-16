from django.core.management.base import BaseCommand
import json
import os
from django.db import models
from django.apps import apps

class Command(BaseCommand):
    help = 'Applies model definitions from JSON files to the models'

    def add_arguments(self, parser):
        parser.add_argument(
            '--input',
            type=str,
            help='Input directory containing model definitions',
            default='model_definitions'
        )
        parser.add_argument(
            '--model',
            type=str,
            help='Specific model to update',
            required=False
        )

    def handle(self, *args, **options):
        input_dir = options['input']
        if not os.path.exists(input_dir):
            self.stdout.write(
                self.style.ERROR(f'Input directory {input_dir} does not exist')
            )
            return

        for filename in os.listdir(input_dir):
            if not filename.endswith('.json'):
                continue

            if options['model'] and filename != f"{options['model']}.json":
                continue

            with open(os.path.join(input_dir, filename), 'r') as f:
                model_def = json.load(f)

            self.update_model(model_def)

    def update_model(self, model_def):
        """Update a model based on its definition"""
        table_name = model_def['table_name']
        model_name = ''.join(word.title() for word in table_name.split('_'))
        
        # Find the model class
        model = None
        for app_config in apps.get_app_configs():
            try:
                model = app_config.get_model(model_name)
                break
            except LookupError:
                continue

        if not model:
            self.stdout.write(
                self.style.WARNING(f'Model {model_name} not found')
            )
            return

        # Update model fields
        for field_def in model_def['fields']:
            field_name = field_def['name']
            if hasattr(model, field_name):
                field = model._meta.get_field(field_name)
                self.update_field(field, field_def)

        self.stdout.write(
            self.style.SUCCESS(f'Successfully updated model {model_name}')
        )

    def update_field(self, field, field_def):
        """Update a field based on its definition"""
        # Update field attributes
        field.null = field_def.get('null', field.null)
        field.blank = field_def.get('blank', field.blank)
        
        if 'default' in field_def:
            field.default = field_def['default']

        # Update field type if needed
        new_type = field_def.get('type')
        if new_type and not isinstance(field, getattr(models, new_type)):
            self.stdout.write(
                self.style.WARNING(
                    f'Field type mismatch for {field.name}: '
                    f'current={field.__class__.__name__}, '
                    f'new={new_type}'
                )
            ) 