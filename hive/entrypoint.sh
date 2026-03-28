#!/bin/bash
set -e

echo "Waiting for Postgres..."

DB_HOST=${DB_HOST:-postgres-metastore}
DB_PORT=${DB_PORT:-5432}
DB_NAME=${HIVE_METASTORE_POSTGRES_DB:-metastore}
DB_USER=${HIVE_METASTORE_POSTGRES_USER:-hiveuser}


until pg_isready -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME"; do
    sleep 2
done

echo "Postgres is available"

# Initialize schema once
if [ ! -f /tmp/schema_initialized ]; then
    schematool -dbType postgres -initOrUpgradeSchema || true
    touch /tmp/schema_initialized
fi

echo "Hive Metastore schema already exists or initialized"

exec /opt/hive/bin/hive --service metastore