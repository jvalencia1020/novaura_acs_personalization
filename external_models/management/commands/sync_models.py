from django.core.management.base import BaseCommand
from django.db import connection
from django.apps import apps
import inspect
import json
import os

class Command(BaseCommand):
    help = 'Synchronizes model definitions from the external application'

    def add_arguments(self, parser):
        parser.add_argument(
            '--app',
            type=str,
            help='Specific app to sync (e.g., crm, accounts)',
            required=False
        )
        parser.add_argument(
            '--model',
            type=str,
            help='Specific model to sync',
            required=False
        )
        parser.add_argument(
            '--output',
            type=str,
            help='Output directory for model definitions',
            default='model_definitions'
        )

    def handle(self, *args, **options):
        # Create output directory if it doesn't exist
        output_dir = options['output']
        os.makedirs(output_dir, exist_ok=True)

        # Get all models from the external database
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT table_name, column_name, data_type, is_nullable, column_default
                FROM information_schema.columns
                WHERE table_schema = DATABASE()
                ORDER BY table_name, ordinal_position;
            """)
            columns = cursor.fetchall()

        # Organize columns by table
        table_columns = {}
        for table, column, data_type, is_nullable, default in columns:
            if table not in table_columns:
                table_columns[table] = []
            table_columns[table].append({
                'name': column,
                'type': data_type,
                'nullable': is_nullable == 'YES',
                'default': default
            })

        # Generate model definitions
        for table, columns in table_columns.items():
            if options['app'] and not table.startswith(options['app']):
                continue

            if options['model'] and table != options['model']:
                continue

            model_def = self.generate_model_definition(table, columns)
            
            # Save to file
            output_file = os.path.join(output_dir, f"{table}.json")
            with open(output_file, 'w') as f:
                json.dump(model_def, f, indent=2)

            self.stdout.write(
                self.style.SUCCESS(f'Successfully generated model definition for {table}')
            )

    def generate_model_definition(self, table, columns):
        """Generate a model definition from table columns"""
        model_def = {
            'table_name': table,
            'fields': [],
            'meta': {
                'db_table': table,
                'managed': False
            }
        }

        for column in columns:
            field_def = {
                'name': column['name'],
                'type': self.map_db_type_to_django(column['type']),
                'null': column['nullable'],
                'blank': column['nullable']
            }

            if column['default']:
                field_def['default'] = column['default']

            model_def['fields'].append(field_def)

        return model_def

    def map_db_type_to_django(self, db_type):
        """Map database types to Django field types"""
        type_mapping = {
            'varchar': 'CharField',
            'text': 'TextField',
            'int': 'IntegerField',
            'bigint': 'BigIntegerField',
            'datetime': 'DateTimeField',
            'date': 'DateField',
            'boolean': 'BooleanField',
            'decimal': 'DecimalField',
            'json': 'JSONField',
            # Add more mappings as needed
        }

        base_type = db_type.split('(')[0].lower()
        return type_mapping.get(base_type, 'CharField') 