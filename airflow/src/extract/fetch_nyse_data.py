import requests
import pandas as pd
import yfinance as yf
from io import StringIO
from src.config.logger import get_logger
from concurrent.futures import ThreadPoolExecutor

# Initialize logger
logger = get_logger(__name__)

# Get the list of NYSE companies from a reliable source
def get_nyse_list():
    nyse_clean_list = []
    nyse_removed_list = []

    df_nyse = pd.read_csv("/content/NYSE_SCREENER_LIST.csv")
    nyse_list = df_nyse['Symbol'].str.strip().tolist()


    for nyse in nyse_list:
        if isinstance(nyse, str):
            if '^' in nyse:
                nyse_removed_list.append(nyse)
                logger.info(f"Removed NYSE symbol: {nyse}")
            else:
                nyse_clean_list.append(nyse)
                logger.info(f"Added NYSE symbol: {nyse}")
    return nyse_clean_list

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

# Extract the profile data for all S&P 500 companies
def extract_nyse_profile():
    '''Extracts the profile data for all NYSE companies.'''
    nyse_list = get_nyse_list()
    data_list = []
    max_workers = 10

    logger.info("Start extracting profile data for NYSE companies.")
    # Fetch profiles in parallel using ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers) as executor:
        results = list(executor.map(fetch_profile, nyse_list))

    logger.info(f"Fetching profiles using {max_workers} threads")

    data_list = [result for result in results if result is not None]

    logger.info(f"Profiles successfully collected: {len(data_list)}")
    logger.info("Finished extracting profile data for NYSE companies.")
    return pd.DataFrame(data_list)
