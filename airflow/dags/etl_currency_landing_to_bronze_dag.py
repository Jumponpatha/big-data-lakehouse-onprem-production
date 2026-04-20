import json
import pandas as pd
from zoneinfo import ZoneInfo
from datetime import datetime, timedelta
from airflow.sdk import task, dag
from src.config.logger import get_logger
from src.s3.minio_s3 import load_data_to_raw_s3
from src.spark.spark_session import create_spark_session
from src.load.load_to_iceberg_s3 import load_raw_data_landing_to_bronze

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
    dag_id="etl_currency_landing_to_bronze_dag",
    schedule="30 8 * * 5", # Every 08:30 on Friday
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["ETL", "Currency" , "Bronze", "Landing"],
    default_args=default_args,
)

# Define the DAG function
def etl_currency_ingestion_landing_to_bronze_dag():
    '''
    # ETL DAG to extract, transform, and load Currency profile data from the landing zone to the bronze zone in the lakehouse.

    The DAG consists of the following tasks:
    1. Extract Currency profile data from the source and add an Ingested_Time column.
    2. Load the extracted Currency profile data to local storage in Parquet format and upload it to the S3 landing zone.
    3. Load the extracted Currency profile data from the landing zone to the bronze zone in the lakehouse using Spark and Iceberg.
    '''

    # TASK 1: Extract Currency profile data from source and add Ingested_Time column
    @task(task_id="extract_currency_data_task")
    def extract_currency_data():

        logger.info("Starting extraction of Currency profile data")
        # Extract Currency profile data from source and add Ingested_Time column
        ingested_time = datetime.now(ZoneInfo("Asia/Bangkok"))
        with open("/opt/airflow/data/source/COMMON_CURRENCY.json") as f:
            data = json.load(f)

        # Convert to DataFrame
        extracted_df = pd.DataFrame.from_dict(data, orient="index").reset_index()

        # Rename index column
        extracted_df.rename(columns={"index": "currency_code"}, inplace=True)
        extracted_df["Ingested_Time"] = ingested_time
        logger.info(f"Shape of extracted Currency profile data: {extracted_df.shape}")
        logger.info(f"Columns in extracted Currency profile data: {extracted_df.columns}")
        logger.info("Finished extraction of Currency profile data")
        return extracted_df

    # TASK 2: Load the extracted Currency profile data to local storage in CSV format
    @task(task_id="load_currency_profiles_to_s3_landing_task")
    def load_currency_profiles_to_s3_landing(df):

        logger.info("Starting upload of Currency profile data to S3")
        logger.info("Converting Currency profile data as CSV format upload to S3 (Landing Zone)")

        bucket_name = 'datalake-landing'
        folder_name = 'currency_profiles'
        datetime_format = datetime.now().strftime('%Y%m%d')
        file_name = f"currency_profiles_{datetime_format}.parquet"
        file_format = "parquet"

        # Upload the Parquet file to S3
        load_data_to_raw_s3(df, file_name, bucket_name, folder_name, file_format)
        logger.info("Finished upload of Currency profile data to S3")

    # TASK 3: Load the extracted Currency profile data from landing zone to bronze zone in the lakehouse using Spark and Iceberg
    @task(task_id="load_currency_profiles_to_bronze_task")
    def load_currency_profiles_to_bronze():
        # Initial Variables
        spark = None
        try:
            # Create Spark Session
            spark = create_spark_session("Extract & Load Currency Profile Data to Bronze Zone")
            logger.info("Starting loading of Currency profile data to bronze zone")

            # Inititalize Variables
            datetime_format = datetime.now().strftime('%Y%m%d')
            file_name = f"currency_profiles_{datetime_format}.parquet"
            s3_path = "datalake-landing/currency_profiles"
            catalog_name = "lakehouse_prod"
            schema_name = "bronze_db"
            table_name = "bronze_currency_profiles"
            load_to_zone = "bronze"
            partition_col = "Ingested_Time"

            # Start loading the Currency profile data from the landing zone to the bronze zone in the lakehouse using Spark and Iceberg
            load_raw_data_landing_to_bronze(spark, s3_path, file_name, load_to_zone, catalog_name, schema_name, table_name, partition_col)

            # Stop the SparkSession
            spark.stop()
            logger.info("Finished loading Currency profile data to bronze zone")
        except Exception as e:
            logger.error(f"Spark job failed: {str(e)}")
            raise
        finally:
            if spark:
                logger.info("Stopping Spark session")
                spark.stop()
                logger.info("Spark session stopped")

    # Define Airflow tasks
    extract_currency_data_task = extract_currency_data()
    load_currency_profiles_to_s3_landing_task = load_currency_profiles_to_s3_landing(extract_currency_data_task)
    load_currency_profiles_to_bronze_task = load_currency_profiles_to_bronze()

    # Define task dependencies
    extract_currency_data_task >> load_currency_profiles_to_s3_landing_task >> load_currency_profiles_to_bronze_task

# Call the DAG function to create the DAG
etl_currency_ingestion_landing_to_bronze_dag()