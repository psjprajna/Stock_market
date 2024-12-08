import yfinance as yf
import pandas as pd
import datetime
import time
import os
from alpha_vantage.timeseries import TimeSeries
from alpha_vantage.fundamentaldata import FundamentalData

# Alpha Vantage API Key
API_KEY = 'RAS8KVR909DWM7KT'
fd = FundamentalData(key=API_KEY, output_format='pandas')
ts = TimeSeries(key=API_KEY, output_format='pandas')

# Create directories if they don't exist
def create_directories():
    os.makedirs('./data/historical', exist_ok=True)
    os.makedirs('./data/realtime', exist_ok=True)

# Fetch historical stock data
def fetch_historical_data(tickers, start_date, end_date):
    """
    Fetch historical stock price and volume data for a list of tickers.
    :param tickers: List of stock symbols
    :param start_date: Start date in 'YYYY-MM-DD' format
    :param end_date: End date in 'YYYY-MM-DD' format
    :return: Dictionary of Pandas DataFrames containing historical stock data
    """
    for ticker in tickers:
        try:
            print(f"Fetching historical data for {ticker}...")
            stock_data = yf.download(ticker, start=start_date, end=end_date)
            stock_data.reset_index(inplace=True)  # Reset the 'Date' index to a column
            stock_data.insert(0, 'Ticker', ticker) 

            # Save data to CSV
            file_path = f'./data/historical/{ticker}_historical_data.csv'
            stock_data.to_csv(file_path, index=False)
            print(f"Saved historical data for {ticker} to {file_path}")

            time.sleep(1)  # Delay to avoid rate limits
        except Exception as e:
            print(f"Error fetching historical data for {ticker}: {e}")

# Fetch real-time stock data and metadata
def fetch_real_time_data_alpha_vantage(tickers):
    """
    Fetch real-time stock data and metadata for a list of tickers.
    :param tickers: List of stock symbols
    """
    for ticker in tickers:
        try:
            print(f"Fetching real-time data for {ticker}...")
            # Fetch real-time data
            real_time_data, _ = ts.get_quote_endpoint(symbol=ticker)
            real_time_df = real_time_data.T  # Transpose real-time data for consistency

            print(f"Fetching metadata for {ticker}...")
            # Fetch metadata
            metadata, _ = fd.get_company_overview(symbol=ticker)
            metadata_df = pd.DataFrame(metadata, index=[0]).T  # Transpose metadata for consistency

            # Combine real-time data and metadata
            combined_data = pd.concat([real_time_df, metadata_df], axis=0)

            # Save combined data to CSV
            file_path = f'./data/realtime/{ticker}_combined_data.csv'
            combined_data.to_csv(file_path, header=False)
            print(f"Saved combined data for {ticker} to {file_path}")

            time.sleep(12)  # Respect API rate limits (5 requests per minute)
        except Exception as e:
            print(f"Error fetching real-time and metadata for {ticker}: {e}")

# Main function to orchestrate fetching and saving data
def main():
    # Define parameters
    tickers = ['AAPL', 'GOOG', 'MSFT', 'AMZN', 'TSLA']  # Extend this list
    start_date = "2020-01-01"
    end_date = datetime.datetime.now().strftime('%Y-%m-%d')

    # Create directories
    create_directories()

    # Fetch and save historical data
    #print("\nFetching and saving historical data...")
    #fetch_historical_data(tickers, start_date, end_date)

    # Fetch and save real-time data and metadata
    print("\nFetching and saving real-time data and metadata...")
    fetch_real_time_data_alpha_vantage(tickers)

if __name__ == "__main__":
    main()
