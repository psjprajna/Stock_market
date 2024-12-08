import os
import time
import pandas as pd
import requests
from alpha_vantage.timeseries import TimeSeries
from alpha_vantage.fundamentaldata import FundamentalData
from alpha_vantage.techindicators import TechIndicators
import logging

# Configure Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# API Key and Directories
API_KEY = "RAS8KVR909DWM7KT"  # Replace with your key
BASE_DIR = "./data/stock_data/"
os.makedirs(BASE_DIR, exist_ok=True)

# Initialize API Clients
ts = TimeSeries(key=API_KEY, output_format="pandas")
fd = FundamentalData(key=API_KEY, output_format="pandas")
ti = TechIndicators(key=API_KEY, output_format="pandas")

# Function to save data
def save_data(data, filename, folder):
    folder_path = os.path.join(BASE_DIR, folder)
    os.makedirs(folder_path, exist_ok=True)
    filepath = os.path.join(folder_path, f"{filename}.csv")
    data.to_csv(filepath)
    logging.info(f"Data saved: {filepath}")

# Fetch and Save Price Data
def fetch_price_data(symbol):
    try:
        logging.info(f"Fetching Core Price Data for {symbol}...")
        intraday, _ = ts.get_intraday(symbol=symbol, interval="1min", outputsize="compact")
        save_data(intraday, f"{symbol}_intraday_price", "price_data")
        time.sleep(12)  # Respect API rate limits
        daily, _ = ts.get_daily(symbol=symbol, outputsize="full")
        save_data(daily, f"{symbol}_daily_price", "price_data")
    except Exception as e:
        logging.error(f"Error fetching price data for {symbol}: {e}")

# Fetch and Save Technical Indicators
def fetch_technical_indicators(symbol):
    try:
        logging.info(f"Fetching Technical Indicators for {symbol}...")
        rsi, _ = ti.get_rsi(symbol=symbol, interval="daily", time_period=14, series_type="close")
        save_data(rsi, f"{symbol}_rsi_indicator", "technical_indicators")
        time.sleep(12)  # Respect API rate limits
        sma, _ = ti.get_sma(symbol=symbol, interval="daily", time_period=50, series_type="close")
        save_data(sma, f"{symbol}_sma_indicator", "technical_indicators")
    except Exception as e:
        logging.error(f"Error fetching technical indicators for {symbol}: {e}")

# Fetch and Save Fundamental Data
def fetch_fundamental_data(symbol):
    try:
        logging.info(f"Fetching Fundamental Data for {symbol}...")
        income_statement, _ = fd.get_income_statement_annual(symbol=symbol)
        save_data(income_statement, f"{symbol}_income_statement", "fundamental_data")
        time.sleep(12)  # Respect API rate limits
        balance_sheet, _ = fd.get_balance_sheet_annual(symbol=symbol)
        save_data(balance_sheet, f"{symbol}_balance_sheet", "fundamental_data")
        time.sleep(12)
        cash_flow, _ = fd.get_cash_flow_annual(symbol=symbol)
        save_data(cash_flow, f"{symbol}_cash_flow_statement", "fundamental_data")
    except Exception as e:
        logging.error(f"Error fetching fundamental data for {symbol}: {e}")

# Fetch and Save Sector Data
def fetch_sector_data():
    try:
        logging.info("Fetching Sector Performance Data...")
        url = f"https://www.alphavantage.co/query?function=SECTOR&apikey={API_KEY}"
        response = requests.get(url)
        response.raise_for_status()  # Raise HTTP errors if any
        data = response.json()

        # Convert to DataFrame
        sector_df = pd.DataFrame.from_dict(data, orient="index").transpose()
        save_data(sector_df, f"{symbol}_sector_performance", "sector_data")
    except Exception as e:
        logging.error(f"Error fetching sector data: {e}")

# Main Function to Fetch All Data
def fetch_all_data(symbol):
    fetch_price_data(symbol)
    fetch_technical_indicators(symbol)
    fetch_fundamental_data(symbol)
    fetch_sector_data()

# Example Usage
if __name__ == "__main__":
    symbols = ["AAPL", "GOOG", "MSFT"]
    for symbol in symbols:
        logging.info(f"Fetching all data for {symbol}...")
        fetch_all_data(symbol)
