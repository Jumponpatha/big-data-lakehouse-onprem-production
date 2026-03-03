import requests
import pandas as pd
import yfinance as yf
from io import StringIO
from airflow import DAG
from datetime import datetime, timedelta
from airflow.sdk import task, dag

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

@dag(
    dag_id="00_etl_get_sp500_profile",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["test"],
)

def etl_get_sp500_profile_dag():
    @task
    def run_etl():
        df = extract_sp500_profile()
        print(df.head())
        df.to_csv("/opt/airflow/data/sp500_profiles.csv", index=False)

    run_etl()


etl_get_sp500_profile_dag()