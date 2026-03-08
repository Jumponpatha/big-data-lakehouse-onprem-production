import requests
import pandas as pd
import yfinance as yf
from zoneinfo import ZoneInfo
from io import BytesIO, StringIO
from datetime import datetime, timedelta
from airflow.sdk import task, dag
import boto3
from botocore.exceptions import NoCredentialsError
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark import SparkConf

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
    dag_id="etl_get_sp500_profile_process_dag_dev",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["ETL", "S&P 500", "S&P 500 Profile" , "Bronze", "Silver", "Gold"],
    default_args=default_args,
)

def etl_get_sp500_profile_process_dag():


etl_get_sp500_profile_process_dag()