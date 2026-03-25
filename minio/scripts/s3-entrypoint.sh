#!/bin/sh

set -e

echo "Configuring MinIO Client (mc)..."

mc alias set minio-prod http://minio:9000 "$AWS_S3_ACCESS_KEY_ID" "$AWS_S3_SECRET_ACCESS_KEY"

mc mb minio-prod/lakehouse-prod-bucket --ignore-existing
mc mb minio-prod/lakehouse-dev-bucket --ignore-existing
mc mb minio-prod/lakehouse-test-bucket --ignore-existing
mc mb minio-prod/lakehouse-gold-bucket --ignore-existing
mc mb minio-prod/lakehouse-silver-bucket --ignore-existing
mc mb minio-prod/lakehouse-bronze-bucket --ignore-existing
mc mb minio-prod/datalake-landing --ignore-existing

echo "✅ All MinIO buckets are ready."