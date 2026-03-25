#!/bin/sh

set -e

echo "Start Configuring MinIO Client (mc)..."

echo "Set Up Alias with S3"
mc alias set minio-prod http://minio:9000 "$AWS_S3_ACCESS_KEY_ID" "$AWS_S3_SECRET_ACCESS_KEY"

echo "Start Creating Bucket"
mc mb minio-prod/lakehouse-prod-bucket --ignore-existing
mc tag set minio-prod/lakehouse-prod-bucket "ENV:PROD&LAYER:WAREHOUSE"

mc mb minio-prod/lakehouse-dev-bucket --ignore-existing
mc tag set minio-prod/lakehouse-prod-bucket "ENV:DEV&LAYER:WAREHOUSE"

mc mb minio-prod/lakehouse-test-bucket --ignore-existing
mc tag set minio-prod/lakehouse-prod-bucket "ENV:TEST&LAYER:WAREHOUSE"

mc mb minio-prod/lakehouse-gold-bucket --ignore-existing
mc tag set minio-prod/lakehouse-prod-bucket "ENV:PROD&LAYER:GOLD&STATE:ANALYTICS-DW"

mc mb minio-prod/lakehouse-silver-bucket --ignore-existing
mc tag set minio-prod/lakehouse-prod-bucket "ENV:PROD&LAYER:SILVER&STATE:TRANSFORMATION-STAGING"

mc mb minio-prod/lakehouse-bronze-bucket --ignore-existing
mc tag set minio-prod/lakehouse-prod-bucket "ENV:PROD&LAYER:BRONZE&STATE:NONTRANSFORMATION-LANDING"

mc mb minio-prod/datalake-landing --ignore-existing
mc tag set minio-prod/lakehouse-prod-bucket "ENV:PROD&LAYER:DATASOURCE&STATE:LANDING"

echo "Finish Creating Bucket"

echo "This is all created buckets"
mc mb ls minio-prod/*

echo "All MinIO buckets are ready."