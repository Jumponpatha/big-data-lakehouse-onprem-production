from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from airflow.sdk import task, dag
from src.extract.fetch_nasdaq_data import extract_nasdaq_profile
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
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'execution_timeout': timedelta(hours=2)
}

# Define the DAG using the @dag decorator
@dag(
    dag_id="etl_nasdaq_profile_landing_to_bronze_dag_dev",
    schedule="@daily",
    catchup=False,
    tags=["ETL", "NASDAQ", "NASDAQ Profile" , "Bronze", "Landing"],
    default_args=default_args,
)

# Define the DAG function
def etl_nasdaq_profile_landing_to_bronze_dag():

    # TASK 1: Extract and load NASDAQ profile data from API into S3 Landing Zone
    @task(task_id="extract_load_nasdaq_from_api_to_s3", execution_timeout=timedelta(minutes=30))
    def extract_load_nasdaq_from_api_to_s3():
        logger.info("Starting extraction of NASDAQ profile data")
        # Extract NASDAQ profile data from source and add Ingested_Time column
        extracted_df = extract_nasdaq_profile()
        ingested_time = datetime.now(ZoneInfo("Asia/Bangkok"))
        extracted_df["Ingested_Time"] = ingested_time

        logger.info("Finished extraction of NASDAQ profile data")
        logger.info(f"Shape of extracted NASDAQ profile data: {extracted_df.shape}")
        logger.info(f"Columns in extracted NASDAQ profile data: {extracted_df.columns}")

        logger.info("Starting upload of NASDAQ profile data to S3")
        logger.info("Converting NASDAQ profile data as Parquet format upload to S3 (Landing Zone)")

        # Define S3 bucket, folder, file name, and format
        bucket_name = 'datalake-landing'
        folder_name = 'nasdaq_profiles'
        file_name = f"nasdaq_profiles_{datetime.now().strftime('%Y%m%d')}.parquet"
        file_format = "parquet"

        # Load the Parquet file to S3
        load_data_to_raw_s3(extracted_df, file_name, bucket_name, folder_name, file_format)
        logger.info("Finished upload of NASDAQ profile data to S3")


    @task(task_id="load_nasdaq_profiles_to_bronze_task")
    def load_nasdaq_profiles_to_bronze():
        try:
            # Create Spark Session
            spark = create_spark_session("Extract & Load NASDAQ Profile Data to Bronze Zone")
            logger.info("Starting loading of NASDAQ profile data to bronze zone")

            file_name = f"nasdaq_profiles_{datetime.now().strftime('%Y%m%d')}.parquet"
            s3_path = "datalake-landing/nasdaq_profiles"
            catalog_name = "lakehouse_prod"
            schema_name = "bronze_db"
            table_name = "bronze_nasdaq_profiles"
            load_to_zone = "bronze"

            # Start loading the NASDAQ profile data from the landing zone to the bronze zone in the lakehouse using Spark and Iceberg
            load_raw_data_landing_to_bronze(spark, s3_path, file_name, load_to_zone, catalog_name, schema_name, table_name)

            # Stop the SparkSession
            spark.stop()
            logger.info("Finished loading NASDAQ profile data to bronze zone")
        except Exception as e:
            logger.error(f"Spark job failed: {str(e)}")
            raise
        finally:
            if spark:
                logger.info("Stopping Spark session")
                spark.stop()
                logger.info("Spark session stopped")

    # Define Airflow tasks
    extract_load_nasdaq_from_api_to_s3_task = extract_load_nasdaq_from_api_to_s3()
    load_nasdaq_profiles_to_bronze_task = load_nasdaq_profiles_to_bronze()
    # Define task dependencies
    extract_load_nasdaq_from_api_to_s3_task >> load_nasdaq_profiles_to_bronze_task

etl_nasdaq_profile_landing_to_bronze_dag()