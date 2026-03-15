import os
from pyspark.sql import SparkSession
from src.config.logger import get_logger
from src.config.settings import settings

# Initialize logger
logger = get_logger(__name__)

# Function to create and configure SparkSession
def create_spark_session(app_name: str) -> SparkSession:
    # Spark configuration parameters
    master = settings.SPARK_MASTER
    catalog_name = settings.CATALOG_NAME
    endpoint_url = settings.S3_ENDPOINT
    s3_warehouse_path = settings.S3_WAREHOUSE_PATH
    hive_metastore_uri = settings.HIVE_METASTORE_URI

    spark_number_partition_sql = "50"
    s3_ssl_enabled = "false"
    s3_path_style_access = "true"

    # Create SparkSession with Iceberg and S3 configurations
    try:
        spark = (
            SparkSession.builder
            .master(master)
            .appName(app_name)
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config(f"spark.sql.catalog.{catalog_name}","org.apache.iceberg.spark.SparkCatalog")
            .config(f"spark.sql.catalog.{catalog_name}.type", "hive")
            .config(f"spark.sql.catalog.{catalog_name}.uri", hive_metastore_uri)
            .config(f"spark.sql.catalog.{catalog_name}.warehouse",s3_warehouse_path)
            .config(f"spark.sql.catalog.{catalog_name}.io-impl","org.apache.iceberg.aws.s3.S3FileIO")
            .config("spark.hadoop.fs.s3a.endpoint", endpoint_url)
            .config("spark.sql.catalog.lakehouse_prod.cache-enabled","false")
            .config("spark.sql.iceberg.handle-timestamp-without-timezone","true")
            .config("spark.hadoop.fs.s3a.path.style.access", s3_path_style_access)
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", s3_ssl_enabled)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.aws.region", "us-east-1")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.sql.shuffle.partitions", spark_number_partition_sql)
            .config("spark.executor.instances", "3")
            .config("spark.executor.cores", "1")
            .config("spark.driver.memory", "1g")
            .config("spark.executor.memory", "1g")
            # Scheduling Mode
            .config("spark.scheduler.mode", "FAIR") # Distribute resources fairly among jobs
            .enableHiveSupport()
            .getOrCreate()
        )

        spark.sparkContext.setLogLevel("ERROR")
        logger.info(f"Starting SparkSession with master: {master}")
        logger.info(f"NOTE: SparkSession app '{app_name}' created successfully!")
    except Exception as e:
        logger.error(f"Error creating SparkSession: {e}")
        raise
    return spark
