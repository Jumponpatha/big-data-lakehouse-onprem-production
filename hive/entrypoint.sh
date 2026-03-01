#!/bin/bash
set -e

echo "Waiting for Postgres..."
until pg_isready -h postgres-metastore -U hiveuser; do
    sleep 2
done

echo "Postgres is available"

# Initialize schema once
if [ ! -f /tmp/schema_initialized ]; then
    schematool -dbType postgres -initOrUpgradeSchema || true
    touch /tmp/schema_initialized
fi

echo "Hive Metastore schema already exists or initialized"

# IMPORTANT: do NOT start HiveServer2 here
exec /opt/hive/bin/hive --service ${SERVICE_NAME}
