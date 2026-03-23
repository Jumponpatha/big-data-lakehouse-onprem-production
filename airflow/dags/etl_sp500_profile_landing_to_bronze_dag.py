from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from airflow.sdk import task, dag
from src.extract.fetch_sp500_profile_data import extract_sp500_profile
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
    'execution_timeout': timedelta(minutes=5)
}

# Define the DAG using the @dag decorator
@dag(
    dag_id="etl_sp500_profile_landing_to_bronze_dag_dev",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["ETL", "S&P 500", "S&P 500 Profile" , "Bronze", "Landing"],
    default_args=default_args,
)

# Define the DAG function
def etl_sp500_profile_ingestion_to_bronze_dag():

    # TASK 1: Extract S&P 500 profile data from source and add Ingested_Time column
    @task(task_id="extract_sp500_profiles_task")
    def extract_sp500_profiles():
        logger.info("Starting extraction of S&P 500 profile data")
        # Extract S&P 500 profile data from source and add Ingested_Time column
        extracted_df = extract_sp500_profile()
        ingested_time = datetime.now(ZoneInfo("Asia/Bangkok"))
        extracted_df["Ingested_Time"] = ingested_time

        logger.info("Finished extraction of S&P 500 profile data")
        logger.info(f"Shape of extracted S&P 500 profile data: {extracted_df.shape}")
        logger.info(f"Columns in extracted S&P 500 profile data: {extracted_df.columns}")
        return extracted_df

    # TASK 2: Load the extracted S&P 500 profile data to local storage in CSV format
    @task(task_id="load_sp500_profiles_to_s3_landing_task")
    def load_sp500_profiles_to_s3_landing(df):

        logger.info("Starting upload of S&P 500 profile data to S3")
        logger.info("Converting S&P 500 profile data as Parquet format upload to S3 (Landing Zone)")

        bucket_name = 'datalake-landing'
        folder_name = 'sp500_profiles'
        file_name = f"sp500_profiles_{datetime.now().strftime('%Y%m%d')}.parquet"
        file_format = "parquet"

        # Upload the Parquet file to S3
        load_data_to_raw_s3(df, file_name, bucket_name, folder_name, file_format)
        logger.info("Finished upload of S&P 500 profile data to S3")

    # TASK 3: Extract the S&P 500 profile data from the landing zone and load it to the bronze zone in the lakehouse
    @task(task_id="load_sp500_profile_landing_to_bronze_task")
    def load_sp500_profile_landing_to_bronze():
        try:
            # Create Spark Session
            spark = create_spark_session("Extract & Load S&P 500 Profile Data to Bronze Zone")
            logger.info("Starting loading of S&P 500 profile data to bronze zone")

            file_name = f"sp500_profiles_{datetime.now().strftime('%Y%m%d')}.parquet"
            s3_path = "datalake-landing/sp500_profiles"
            catalog_name = "lakehouse_prod"
            schema_name = "bronze_db"
            table_name = "bronze_sp500_profiles"
            load_to_zone = "bronze"

            # Start loading the S&P 500 profile data from the landing zone to the bronze zone in the lakehouse using Spark and Iceberg
            load_raw_data_landing_to_bronze(spark, s3_path, file_name, load_to_zone, catalog_name, schema_name, table_name)

            # Stop the SparkSession
            spark.stop()
            logger.info("Finished loading S&P 500 profile data to bronze zone")
        except Exception as e:
            logger.error(f"Spark job failed: {str(e)}")
            raise
        finally:
            if spark:
                logger.info("Stopping Spark session")
                spark.stop()
                logger.info("Spark session stopped")

    # Define Airflow tasks
    extract_sp500_profiles_task = extract_sp500_profiles()
    load_sp500_profiles_to_s3_landing_task = load_sp500_profiles_to_s3_landing(extract_sp500_profiles_task)
    load_sp500_profile_landing_to_bronze_task = load_sp500_profile_landing_to_bronze()

    # Define task dependencies
    extract_sp500_profiles_task >> load_sp500_profiles_to_s3_landing_task >> load_sp500_profile_landing_to_bronze_task
etl_sp500_profile_landing_to_bronze_dag()