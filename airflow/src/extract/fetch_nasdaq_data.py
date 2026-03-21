import requests
import time
import random
import pandas as pd
import numpy as np
import yfinance as yf
from io import StringIO
from src.config.logger import get_logger
from concurrent.futures import ThreadPoolExecutor

# Initialize logger
logger = get_logger(__name__)

# Get the list of NASDAQ companies from a reliable source
def get_nasdaq_list():
    df = pd.read_csv("/opt/airflow/src/extract/ref/NASDAQ_SCREENER_LIST.csv")
    df_nasdaq = df[df['Symbol'].notna()]

    df_nasdaq = df_nasdaq[~df_nasdaq['Symbol'].str.contains(r'[\^]', na=False)]

    df_nasdaq = df_nasdaq[~df_nasdaq['Name'].str.contains(
        'Warrant|Rights|Units', case=False, na=False
    )]

    df_nasdaq = df_nasdaq[df_nasdaq['Industry'] != 'Blank Checks']

    df_nasdaq = df_nasdaq[df_nasdaq['Market Cap'] > 50_000_000]

    df_nasdaq = df_nasdaq[
        df_nasdaq['Sector'].notna() &
        df_nasdaq['Industry'].notna()
    ]

    df_nasdaq = df_nasdaq[df_nasdaq['Volume'] > 10000]

    nasdaq_list = df_nasdaq['Symbol'].str.strip().tolist()
    logger.info(f"Retrieved NASDAQ list with {len(nasdaq_list)} companies")
    return nasdaq_list

# Fetch the profile data for a given stock symbol
def fetch_profile(symbol):
    try:
        time.sleep(random.uniform(0.5, 1.0))

        ticker = yf.Ticker(symbol)
        info = ticker.info

        if not info:
            return "NO_DATA"

        market_cap = info.get("marketCap", 0)

        if market_cap is None or market_cap == 0:
            return "FILTERED"

        info["symbol"] = symbol
        return info

    except Exception as e:
        logger.error(f"Failed to fetch profile for {symbol}: {e}")
        return "ERROR"

# Extract the profile data for all NASDAQ companies
def fetch_with_retry(symbol, retries=3):
    for i in range(retries):
        result = fetch_profile(symbol)

        if isinstance(result, dict):
            return result  # Success

        if result in ["NO_DATA", "FILTERED"]:
            return None  # Don't retry

        # only retry on real errors
        wait = 2 ** i
        logger.warning(f"Retry {symbol} in {wait}s")
        time.sleep(wait)

    return None

def chunk_list(data, size):
    for i in range(0, len(data), size):
        yield data[i:i + size]

def extract_nasdaq_profile():
    '''Extracts the profile data for all NASDAQ companies.'''
    nasdaq_list = get_nasdaq_list()

    batch_size = 50
    max_workers = 3
    all_results = []

    logger.info(f"Start processing batch of {batch_size} symbols")
    logger.info(f"Thread Pool Size: {max_workers} threads")
    for batch_idx, batch in enumerate(chunk_list(nasdaq_list, batch_size), start=1):
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            results = list(executor.map(fetch_with_retry, batch))

        all_results.extend([r for r in results if r])
        logger.info(f"[Batch {batch_idx}] Finished processing {len(batch)} symbols. Total records: {len(all_results)}")
        time.sleep(1)  # Cooldown between batches

    logger.info(f"Profiles successfully collected: {len(all_results)}")
    logger.info("Finished extracting profile data for NASDAQ companies.")
    df = pd.DataFrame(all_results)

    # Replace problematic values
    df = df.replace(
        ["Infinity", "-Infinity", "inf", "-inf"],
        np.nan
    )

    # Replace numeric infinities
    df = df.replace([np.inf, -np.inf], np.nan)

    # Convert numeric columns safely
    for col in df.columns:
        try:
            df[col] = pd.to_numeric(df[col])
        except:
            pass

    return df