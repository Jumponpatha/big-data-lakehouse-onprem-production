import requests
import pandas as pd
import yfinance as yf
from zoneinfo import ZoneInfo
from io import BytesIO, StringIO
from datetime import datetime, timedelta
from airflow.sdk import task, dag
import boto3
from botocore.exceptions import NoCredentialsError

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


@dag(
    dag_id="00_etl_get_sp500_profile_dag",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["ETL", "S&P 500", "Profile"],
    default_args=default_args,
)

def etl_get_sp500_profile_dag():

    def get_sp500_list():
        # Get the list of S&P 500 companies from a reliable source
        url = "https://www.slickcharts.com/sp500"

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }

        response = requests.get(url, headers=headers)
        response.raise_for_status()

        try:
            df = pd.read_html(StringIO(response.text))[0]
            df['Symbol'] = df['Symbol'].str.replace('.', '-', regex=False)

            sp500_list = df['Symbol'].tolist()
            print("S&P 500 company list extracted successfully.")
            print(f"Total companies in S&P 500: {len(sp500_list)}")
            return sp500_list
        except Exception as e:
            print(f"The ERROR is: {e}")

    def extract_sp500_profile():
        sp500_list = get_sp500_list()
        data_list = []

        for symbol in sp500_list:
            try:
                ticker = yf.Ticker(symbol)
                info = ticker.info

                info["symbol"] = symbol
                data_list.append(info)

            except Exception as e:
                print(f"Error for {symbol}: {e}")

        return pd.DataFrame(data_list)

    # Define Airflow tasks
    @task
    def extract_sp500_profiles():
        df = extract_sp500_profile()
        print(df.head())
        return df
    @task
    def load_sp500_profiles_to_local(df):
        file_name = f"/opt/airflow/data/sp500_profiles.csv"
        df.to_csv(file_name, index=False)
        print(f"Data saved to {file_name}")

    @task
    def load_sp500_profiles_to_s3(df):

        s3 = boto3.client(
            "s3",
            endpoint_url="http://minio:9000",
            aws_access_key_id='minioadmin',
            aws_secret_access_key='minioadmin',
            region_name="us-east-1",
        )

        bucket_name = 'datalake-landing'
        file_name = f"profiles/sp500_profiles_{datetime.now().strftime('%Y%m%d')}.parquet"
        parquet_buffer = BytesIO()
        df.to_parquet(parquet_buffer, index=False, engine="pyarrow")

        try:
            s3.put_object(Bucket=bucket_name, Key=file_name, Body=parquet_buffer.getvalue())
            print(f"Data uploaded to S3 bucket '{bucket_name}' with path '{file_name}'")
        except NoCredentialsError:
            print("AWS credentials not found. Please configure your AWS credentials.")

    # Run the ETL tasks
    extract_sp500_profiles_task = extract_sp500_profiles()

    load_sp500_profiles_to_local_task = load_sp500_profiles_to_local(extract_sp500_profiles_task)
    load_sp500_profiles_to_s3_task = load_sp500_profiles_to_s3(extract_sp500_profiles_task)


    extract_sp500_profiles_task >> [load_sp500_profiles_to_local_task, load_sp500_profiles_to_s3_task]

etl_get_sp500_profile_dag()