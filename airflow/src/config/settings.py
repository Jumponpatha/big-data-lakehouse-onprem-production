import os

class Settings:
    """Application configuration settings."""
    SPARK_MASTER = os.getenv("SPARK_MASTER", "spark://spark-master:7077")
    S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
    CATALOG_NAME = os.getenv("CATALOG_NAME", "lakehouse_prod")
    S3_PATH_STYLE_ACCESS = os.getenv("S3_PATH_STYLE_ACCESS", "true")
    S3_WAREHOUSE_PATH = os.getenv("S3_WAREHOUSE_PATH", "s3a://lakehouse-prod-bucket/warehouse/")
    HIVE_METASTORE_URI = os.getenv("HIVE_METASTORE_URI", "thrift://hive-metastore:9083")

settings = Settings()