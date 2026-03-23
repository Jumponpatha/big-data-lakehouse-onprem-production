#!/bin/bash
set -e

echo "Waiting for Postgres..."

until pg_isready -h postgres-metastore -U ${HIVE_METASTORE_POSTGRES_USER} -d ${HIVE_METASTORE_POSTGRES_DB}; do
    sleep 2
done

echo "Postgres is available"

# Initialize schema once
if [ ! -f /tmp/schema_initialized ]; then
    schematool -dbType postgres -initOrUpgradeSchema || true
    touch /tmp/schema_initialized
fi

echo "Hive Metastore schema already exists or initialized"

exec /opt/hive/bin/hive --service ${SERVICE_NAME}