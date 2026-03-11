import boto3
from io import BytesIO
from datetime import datetime, timedelta
from airflow.sdk import task, dag
from airflow.sdk.bases.hook import BaseHook
from botocore.exceptions import NoCredentialsError
from src.ingestion.get_sp500_profile_data import extract_sp500_profile
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
    dag_id="ingest_sp500_profile_dag",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["ETL", "S&P 500", "Profile"],
    default_args=default_args,
)

# Define the DAG function
def ingest_sp500_profile_dag():

    # Define Airflow tasks
    @task
    def extract_sp500_profiles():
        df = extract_sp500_profile()
        return df
    @task
    def load_sp500_profiles_to_local(df):
        file_name = f"/opt/airflow/data/landing-raw/sp500_profiles.csv"
        df.to_csv(file_name, index=False)
        logger.info(f"Data saved to {file_name}")

    @task
    def load_sp500_profiles_to_s3(df):
        conn = BaseHook.get_connection("minio_s3")

        extra = conn.extra_dejson
        endpoint = extra.get("endpoint_url")

        s3 = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=conn.login,
            aws_secret_access_key=conn.password,
            region_name="us-east-1"
        )

        bucket_name = 'datalake-landing'
        file_name = f"profiles/sp500_profiles_{datetime.now().strftime('%Y%m%d')}.parquet"
        parquet_buffer = BytesIO()
        df.to_parquet(parquet_buffer, index=False, engine="pyarrow")

        try:
            s3.put_object(Bucket=bucket_name, Key=file_name, Body=parquet_buffer.getvalue())
            logger.info(f"Data uploaded to S3 bucket '{bucket_name}' with path '{file_name}'")
        except NoCredentialsError:
            logger.error("AWS credentials not found. Please configure your AWS credentials.")

    # Run the ETL tasks
    extract_sp500_profiles_task = extract_sp500_profiles()

    load_sp500_profiles_to_local_task = load_sp500_profiles_to_local(extract_sp500_profiles_task)
    load_sp500_profiles_to_s3_task = load_sp500_profiles_to_s3(extract_sp500_profiles_task)

    # Set task dependencies
    extract_sp500_profiles_task >> [load_sp500_profiles_to_local_task, load_sp500_profiles_to_s3_task]

ingest_sp500_profile_dag()