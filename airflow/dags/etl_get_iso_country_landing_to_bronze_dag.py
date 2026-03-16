import boto3
from io import BytesIO
import pandas as pd
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from airflow.sdk import task, dag
from src.s3.minio_s3 import load_data_to_raw_s3
from src.load.load_to_iceberg_s3 import load_raw_data_landing_to_bronze
from src.spark.spark_session import create_spark_session
from src.config.logger import get_logger

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
    'execution_timeout': timedelta(minutes=5)
}

# Define the DAG using the @dag decorator
@dag(
    dag_id="etl_iso_country_ingestion_landing_to_bronze_dag_dev",
    schedule="30 21 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["ETL", "ISO Country" , "Bronze", "Landing"],
    default_args=default_args,
)

# Define the DAG function
def etl_iso_country_ingestion_landing_to_bronze_dag():

    # TASK 1: Extract ISO Country profile data from source and add Ingested_Time column
    @task(task_id="extract_iso_country_data_task")
    def extract_iso_country_data():
        logger.info("Starting extraction of ISO Country profile data")
        # Extract ISO Country profile data from source and add Ingested_Time column
        extracted_df = pd.read_csv("/opt/airflow/data/source/countries_iso3166b.csv", encoding='unicode_escape')
        ingested_time = datetime.now(ZoneInfo("Asia/Bangkok"))
        extracted_df["Ingested_Time"] = ingested_time

        logger.info(f"Shape of extracted ISO Country profile data: {extracted_df.shape}")
        logger.info(f"Columns in extracted ISO Country profile data: {extracted_df.columns}")
        logger.info("Finished extraction of ISO Country profile data")
        return extracted_df

    # TASK 2: Load the extracted ISO Country profile data to local storage in CSV format
    @task(task_id="load_iso_country_profiles_to_s3_landing_task")
    def load_iso_country_profiles_to_s3_landing(df):

        logger.info("Starting upload of ISO Country profile data to S3")
        logger.info("Converting ISO Country profile data as CSV format upload to S3 (Landing Zone)")

        bucket_name = 'datalake-landing'
        folder_name = 'iso_country_profiles'
        file_name = f"country_iso_profiles_{datetime.now().strftime('%Y%m%d')}.parquet"
        file_format = "parquet"

        # Upload the Parquet file to S3
        load_data_to_raw_s3(df, file_name, bucket_name, folder_name, file_format)
        logger.info("Finished upload of ISO Country profile data to S3")

    # TASK 3: Load the extracted ISO Country profile data from landing zone to bronze zone in the lakehouse using Spark and Iceberg
    @task(task_id="load_iso_country_profiles_to_bronze_task")
    def load_iso_country_profiles_to_bronze():
        try:
            # Create Spark Session
            spark = create_spark_session("Extract & Load ISO Country Profile Data to Bronze Zone")
            logger.info("Starting loading of ISO Country profile data to bronze zone")

            file_name = f"country_iso_profiles_{datetime.now().strftime('%Y%m%d')}.parquet"
            s3_path = "datalake-landing/iso_country_profiles"
            catalog_name = "lakehouse_prod"
            schema_name = "bronze_db"
            table_name = "bronze_country_iso_profiles"
            load_to_zone = "bronze"

            # Start loading the ISO Country profile data from the landing zone to the bronze zone in the lakehouse using Spark and Iceberg
            load_raw_data_landing_to_bronze(spark, s3_path, file_name, load_to_zone, catalog_name, schema_name, table_name)

            # Stop the SparkSession
            spark.stop()
            logger.info("Finished loading ISO Country profile data to bronze zone")
        except Exception as e:
            logger.error(f"Spark job failed: {str(e)}")
            raise
        finally:
            if spark:
                logger.info("Stopping Spark session")
                spark.stop()
                logger.info("Spark session stopped")

    # Define Airflow tasks
    extract_iso_country_data_task = extract_iso_country_data()
    load_iso_country_profiles_to_s3_landing_task = load_iso_country_profiles_to_s3_landing(extract_iso_country_data_task)
    load_iso_country_profiles_to_bronze_task = load_iso_country_profiles_to_bronze()

    # Define task dependencies
    extract_iso_country_data_task >> load_iso_country_profiles_to_s3_landing_task >> load_iso_country_profiles_to_bronze_task

etl_iso_country_ingestion_landing_to_bronze_dag()