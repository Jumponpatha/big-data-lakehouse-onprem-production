import pandas as pd
from datetime import datetime, timedelta
from airflow.sdk import task, dag
from src.spark.spark_session import create_spark_session
from src.load.load_to_iceberg_s3 import load_to_s3_lakehouse
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
    dag_id="etl_currency_bronze_to_silver_dag_dev",
    schedule="0 9 * * 5", # Every 09:00 on Friday
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["ETL", "Currency", "Bronze", "Silver"],
    default_args=default_args,
)

# Define the DAG function
def etl_currency_bronze_to_silver_dag():
    '''
    # ETL DAG to transform and load Currency profile data from Bronze to Silver zone in the lakehouse

    The DAG consists of the following tasks:
    '''

    @task(task_id="transform_load_currency_bronze_to_silver_task")
    def transform_load_currency_bronze_to_silver():
        logger.info("Starting transformation of Currency profile data from Bronze to Silver zone")

        # Define source and target table details
        catalog_name = "lakehouse_prod"
        fetched_schema_name = "bronze_db"
        fetched_table_name = "bronze_currency_profiles"
        
        # Load data from Bronze zone
        df = s
