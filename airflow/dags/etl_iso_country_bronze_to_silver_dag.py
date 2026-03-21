import pandas as pd
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import json
from airflow.sdk import task, dag
from src.spark.spark_session import create_spark_session
from src.load.load_to_iceberg_s3 import load_to_lakehouse
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
    dag_id="etl_iso_country_bronze_to_silver_dag",
    schedule="30 21 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["ETL", "ISO Country" , "Bronze", "Silver"],
    default_args=default_args,
)

# Define the DAG function
def etl_iso_country_bronze_to_silver_dag():
    '''ETL DAG to transform and load ISO Country profile data from Bronze to Silver zone in the lakehouse'''
    @task(task_id="transform_load_iso_country_bronze_to_silver_task")
    def transform_load_iso_country_bronze_to_silver():
        logger.info("Starting transformation of ISO Country profile data from Bronze to Silver zone")

        catalog_name = "lakehouse_prod"
        fetched_schema_name = "bronze_db"
        fetched_table_name = "bronze_country_iso_profiles"

        try:
            # Create Spark session
            spark = create_spark_session(app_name="ETL_ISO_Country_Bronze_to_Silver_DAG")

            # Check if the source table in the Bronze zone exists before reading the data
            logger.info(f"Checking if source table {catalog_name}.{fetched_schema_name}.{fetched_table_name} exists in the lakehouse")
            if not spark.catalog.tableExists(f"{catalog_name}.{fetched_schema_name}.{fetched_table_name}"):
                raise ValueError(f"Table {catalog_name}.{fetched_schema_name}.{fetched_table_name} does NOT exist")

            # Read the raw data from the Bronze zone in the lakehouse using Spark and Iceberg
            logger.info(f"Reading ISO Country profile data from Bronze zone table: {catalog_name}.{fetched_schema_name}.{fetched_table_name}")
            df = spark.read.format("iceberg").load(f"{catalog_name}.{fetched_schema_name}.{fetched_table_name}")
            logger.info(f"Fetched {catalog_name}.{fetched_schema_name}.{fetched_table_name} data with {df.count()} rows and columns: {df.columns}")

            # Cache the DataFrame to optimize subsequent transformations
            df = df.cache()
            # Drop duplicates if any in Yahoo Finance API
            df = df.dropDuplicates()
            dedup_count = df.count()
            logger.info(f"Number of rows after dropping duplicates: {dedup_count}")

            # Transform the data by renaming columns, adding new columns, and creating a surrogate key
            logger.info("Transforming ISO Country profile data by renaming columns, adding new columns, and creating a surrogate key")
            transformed_df = df\
                .withColumnRenamed('country', 'COUNTRY_NAME')\
                .withColumnRenamed('country_common', 'COUNTRY_FULL_NAME')\
                .withColumnRenamed('iso2', 'COUNTRY_ISO_CODE_2')\
                .withColumnRenamed('iso3', 'COUNTRY_ISO_CODE_3')\
                .withColumnRenamed('iso_num', 'COUNTRY_NUM_CODE')\
                .withColumnRenamed('Ingested_Time', 'INGESTED_TIME')\

            # Add new columns for processing time and surrogate key
            transformed_df = transformed_df.withColumn(
                "COUNTRY_KEY",
                lpad(col("COUNTRY_NUM_CODE").cast("string"), 4, "0")
            )

            logger.info("Finished transformation of ISO Country profile data from Bronze to Silver zone")
            transformed_df =transformed_df.select(
                "COUNTRY_KEY",
                "COUNTRY_NAME",
                "COUNTRY_FULL_NAME",
                "COUNTRY_ISO_CODE_2",
                "COUNTRY_ISO_CODE_3",
                "COUNTRY_NUM_CODE",
                "INGESTED_TIME"
            )

            logger.info(f"Schema of transformed iso country profile data: {transformed_df.schema}")

            catalog_name = "lakehouse_prod"
            loaded_schema_name = "silver_db"
            loaded_table_name = "silver_country_iso_profiles"
            warehouse_iceberg_directory = f"s3a://lakehouse-silver-bucket/warehouse/{loaded_schema_name}/{loaded_table_name}/"

            # Load the transformed data to the Silver zone in the lakehouse using Spark and Iceberg
            load_to_lakehouse(transformed_df, catalog_name, loaded_schema_name, loaded_table_name, warehouse_iceberg_directory)
            logger.info("Finished loading transformed ISO Country profile data to Silver zone")
        except Exception as e:
            logger.error(f"Spark job failed: {str(e)}")
            raise
        finally:
            if spark:
                logger.info("Stopping Spark session")
                spark.stop()
                logger.info("Spark session stopped")

    # Define Airflow tasks
    transform_load_iso_country_bronze_to_silver_task = transform_load_iso_country_bronze_to_silver()

    # Define task dependencies
    transform_load_iso_country_bronze_to_silver_task

# Call the DAG function to create the DAG
etl_iso_country_bronze_to_silver_dag()