import pandas as pd
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import json
from airflow.sdk import task, dag
from src.spark.spark_session import create_spark_session
from src.load.load_to_iceberg_s3 import load_to_s3_lakehouse
from src.config.logger import get_logger
from pyspark.sql.functions import col, lpad

# Initialize logger
logger = get_logger(__name__)

# Default arguments for the DAG
default_args = {
    'owner': 'Jumponpatha | Data Engineer',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email': ['jumponpat59@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'execution_timeout': timedelta(minutes=15)
}

# Define the DAG using the @dag decorator
@dag(
    dag_id="etl_iso_country_silver_to_gold_dag_dev",
    schedule="30 21 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["ETL", "ISO Country" , "Silver", "Gold"],
    default_args=default_args,
)

# Define the DAG function
def etl_iso_country_silver_to_gold_dag():
    '''ETL DAG to transform and load ISO Country profile data from Silver to Gold zone in the lakehouse'''
    @task(task_id="transform_load_iso_country_silver_to_gold_task")
    def transform_load_iso_country_silver_to_gold():
        logger.info("Starting transformation of ISO Country profile data from Silver to Gold zone")

        catalog_name = "lakehouse_prod"
        fetched_schema_name = "silver_db"
        fetched_table_name = "silver_country_iso_profiles"

        try:
            # Create Spark session
            spark = create_spark_session(app_name="ETL_ISO_Country_Silver_to_Gold_DAG")

            # Check if the source table in the Silver zone exists before reading the data
            logger.info(f"Checking if source table {catalog_name}.{fetched_schema_name}.{fetched_table_name} exists in the lakehouse")
            if not spark.catalog.tableExists(f"{catalog_name}.{fetched_schema_name}.{fetched_table_name}"):
                raise ValueError(f"Table {catalog_name}.{fetched_schema_name}.{fetched_table_name} does NOT exist")

            # Read the raw data from the Silver zone in the lakehouse using Spark and Iceberg
            logger.info(f"Reading ISO Country profile data from Silver zone table: {catalog_name}.{fetched_schema_name}.{fetched_table_name}")
            df = spark.read.format("iceberg").load(f"{catalog_name}.{fetched_schema_name}.{fetched_table_name}")
            logger.info(f"Fetched {catalog_name}.{fetched_schema_name}.{fetched_table_name} data with {df.count()} rows and columns: {df.columns}")

            catalog_name = "lakehouse_prod"
            loaded_schema_name = "gold_db"
            loaded_table_name = "gold_country_iso_profiles"
            warehouse_iceberg_directory = f"s3a://lakehouse-gold-bucket/warehouse/{loaded_schema_name}/{loaded_table_name}/"

            # Load the transformed data to the Gold zone in the lakehouse using Spark and Iceberg
            load_to_s3_lakehouse(df, catalog_name, loaded_schema_name, loaded_table_name, warehouse_iceberg_directory)
            logger.info("Finished loading transformed ISO Country profile data to Gold zone")
        except Exception as e:
            logger.error(f"Spark job failed: {str(e)}")
            raise
        finally:
            if spark:
                logger.info("Stopping Spark session")
                spark.stop()
                logger.info("Spark session stopped")

    # Define Airflow tasks
    transform_load_iso_country_silver_to_gold_task = transform_load_iso_country_silver_to_gold()

    # Define task dependencies
    transform_load_iso_country_silver_to_gold_task

# Call the DAG function to create the DAG
etl_iso_country_silver_to_gold_dag()