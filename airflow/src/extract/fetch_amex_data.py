import requests
import pandas as pd
import yfinance as yf
from io import StringIO
from src.config.logger import get_logger
from concurrent.futures import ThreadPoolExecutor

# Initialize logger
logger = get_logger(__name__)

# Get the list of AMEX companies from a reliable source
def get_amex_list():
    df_amex = pd.read_csv("/content/AMEX_SCREENER_LIST.csv")
    amex_list = df_amex['Symbol'].str.strip().tolist()
    len(amex_list)
    return amex_list

# Fetch the profile data for a given stock symbol
def fetch_profile(symbol):
    '''Fetches the profile data for a given stock symbol.'''
    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info
        logger.info(f"Profile fetched for {symbol}")
        info["symbol"] = symbol
        # Log a warning if the profile data is empty or missing key fields
        if info is None:
            logger.warning(f"No valid data returned for {symbol}")
        return info
    except Exception as e:
        logger.error(f"Failed to fetch profile for {symbol}: {e}")
        return None

# Extract the profile data for all AMEX companies
def extract_amex_profile():
    '''Extracts the profile data for all Amex companies.'''
    amex_list = get_amex_list()
    data_list = []
    max_workers = 10

    logger.info("Start extracting profile data for AMEX companies.")
    # Fetch profiles in parallel using ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers) as executor:
        results = list(executor.map(fetch_profile, amex_list))

    logger.info(f"Fetching profiles using {max_workers} threads")

    data_list = [result for result in results if result is not None]

    logger.info(f"Profiles successfully collected: {len(data_list)}")
    logger.info("Finished extracting profile data for AMEX companies.")
    return pd.DataFrame(data_list)
