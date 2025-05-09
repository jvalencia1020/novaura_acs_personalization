#!/bin/bash
set -e

# Wait for the database to be ready
echo "Waiting for database..."
python -c "
import sys
import time
import pymysql
while True:
    try:
        pymysql.connect(
            host='$DB_HOST',
            user='$DB_USER',
            password='$DB_PASSWORD',
            database='$DB_NAME'
        )
        break
    except pymysql.OperationalError:
        sys.stderr.write('Database not ready yet. Waiting...\n')
        time.sleep(1)
"
echo "Database is ready!"

# Start the specified service
case "$SERVICE_TYPE" in
  "scheduler")
    echo "Starting scheduler service..."
    python manage.py run_scheduler
    ;;
  "worker")
    echo "Starting SQS worker service..."
    python manage.py run_worker
    ;;
  *)
    echo "Unknown service type: $SERVICE_TYPE"
    exit 1
    ;;
esac