#!/bin/sh
set -e

echo "Start Configuring MinIO Client (mc)..."

echo "Waiting for MinIO..."
until mc alias set minio-prod http://minio:9000 "$AWS_S3_ACCESS_KEY_ID" "$AWS_S3_SECRET_ACCESS_KEY"; do
    echo "Retrying..."
    sleep 2
done

echo "MinIO is ready"

echo "Creating buckets..."

mc mb minio-prod/lakehouse-prod-bucket --ignore-existing
mc tag set minio-prod/lakehouse-prod-bucket "ENV=PROD&LAYER=WAREHOUSE"

mc mb minio-prod/lakehouse-dev-bucket --ignore-existing
mc tag set minio-prod/lakehouse-dev-bucket "ENV=DEV&LAYER=WAREHOUSE"

mc mb minio-prod/lakehouse-test-bucket --ignore-existing
mc tag set minio-prod/lakehouse-test-bucket "ENV=TEST&LAYER=WAREHOUSE"

mc mb minio-prod/lakehouse-gold-bucket --ignore-existing
mc tag set minio-prod/lakehouse-gold-bucket "ENV=PROD&LAYER=GOLD"

mc mb minio-prod/lakehouse-silver-bucket --ignore-existing
mc tag set minio-prod/lakehouse-silver-bucket "ENV=PROD&LAYER=SILVER"

mc mb minio-prod/lakehouse-bronze-bucket --ignore-existing
mc tag set minio-prod/lakehouse-bronze-bucket "ENV=PROD&LAYER=BRONZE"

mc mb minio-prod/datalake-landing --ignore-existing
mc tag set minio-prod/datalake-landing "ENV=PROD&LAYER=DATASOURCE"

echo "Buckets created:"
mc ls minio-prod

echo "All MinIO buckets are ready."