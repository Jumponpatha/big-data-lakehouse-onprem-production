import requests
import pandas as pd
import yfinance as yf
from io import StringIO
from src.config.logger import get_logger
from concurrent.futures import ThreadPoolExecutor

# Initialize logger
logger = get_logger(__name__)

# Get the list of S&P 500 companies from a reliable source
def get_sp500_list():
    '''Fetches the list of S&P 500 companies from SlickCharts.'''
    url = "https://www.slickcharts.com/sp500"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }

    response = requests.get(url, headers=headers)
    response.raise_for_status()

    # Parse the HTML to extract the S&P 500 company symbols
    try:
        df = pd.read_html(StringIO(response.text))[0]
        df['Symbol'] = df['Symbol'].str.replace('.', '-', regex=False)
        sp500_list = df['Symbol'].tolist()

        logger.info("S&P 500 company list extracted successfully.")
        logger.info(f"Total companies in S&P 500: {len(sp500_list)}")
        return sp500_list
    except Exception as e:
        logger.error(f"The ERROR is: {e}")

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
def extract_sp500_profile():
    '''Extracts the profile data for all S&P 500 companies.'''
    sp500_list = get_sp500_list()
    data_list = []
    max_workers = 10

    logger.info("Start extracting profile data for S&P 500 companies.")
    # Fetch profiles in parallel using ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers) as executor:
        results = list(executor.map(fetch_profile, sp500_list))

    logger.info(f"Fetching profiles using {max_workers} threads")

    data_list = [result for result in results if result is not None]

    logger.info(f"Profiles successfully collected: {len(data_list)}")
    logger.info("Finished extracting profile data for S&P 500 companies.")
    return pd.DataFrame(data_list)
