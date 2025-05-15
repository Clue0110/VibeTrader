import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import math



def fetch_and_save_stock_data_10_years(ticker_symbol: str, file_path: str = None) -> bool:
    """
    Fetches the last 10 years of historical stock data for a given ticker symbol
    and saves it to a CSV file.

    Args:
        ticker_symbol (str): The stock ticker symbol (e.g., "AAPL" for Apple).
        file_path (str, optional): The full path (including filename) to save the CSV.
                                     If None, defaults to "TICKER_10_years_data.csv"
                                     in the current working directory.

    Returns:
        bool: True if data was successfully fetched and saved, False otherwise.
    """
    # Calculate the end date (today) and start date (10 years ago)
    end_date = datetime.now() - timedelta(days=1)
    start_date = end_date - timedelta(days=10*365.25) # Using 365.25 to account for leap years

    # Format dates as YYYY-MM-DD strings, which yfinance expects
    end_date_str = end_date.strftime('%Y-%m-%d')
    start_date_str = start_date.strftime('%Y-%m-%d')

    print(f"Fetching data for {ticker_symbol} from {start_date_str} to {end_date_str}...")

    try:
        # Download stock data
        stock_data = yf.download(ticker_symbol, start=start_date_str, end=end_date_str, interval='1d', progress=False)

        if stock_data.empty:
            print(f"No data found for ticker {ticker_symbol} for the last 10 years.")
            return False

        # Add a Ticker column for easier identification if concatenating multiple CSVs later
        stock_data['Ticker'] = ticker_symbol

        # Determine the filename
        if file_path is None:
            file_name = f"{ticker_symbol.upper()}_10_years_data.csv"
        else:
            file_name = file_path
            # Ensure the directory exists if a full path is given (optional, basic check)
            # import os
            # os.makedirs(os.path.dirname(file_name), exist_ok=True)


        # Save the DataFrame to a CSV file
        stock_data.to_csv(file_name)
        print(f"Data for {ticker_symbol} successfully saved to {file_name}")
        return True

    except Exception as e:
        print(f"An error occurred while fetching or saving data for {ticker_symbol}: {e}")
        return False
    
def fetch_prev_stock_data(ticker_symbol: str, curr_date: datetime = None, period: int = 0, freq:str = "d", return_sequence:bool = True):
    """
    Fetches the last 10 years of historical stock data for a given ticker symbol
    and saves it to a CSV file.

    Args:
        ticker_symbol (str): The stock ticker symbol (e.g., "AAPL" for Apple).
        file_path (str, optional): The full path (including filename) to save the CSV.
                                     If None, defaults to "TICKER_10_years_data.csv"
                                     in the current working directory.

    Returns:
        Dataframe: A dataframe containing the values.
    """
    # Calculate the end date (today) and start date (10 years ago)
    new_period = period+int(math.ceil(period*2/7))+((int(period/360)+1)*12)
    print(f"New Period: {new_period}")
    end_date = datetime.now() - timedelta(days=1) if curr_date==None else curr_date
    interval="1d"
    if freq=="y":
        start_date = end_date - timedelta(years=new_period*365.25) # Using 365.25 to account for leap years
    elif freq=="d":
        start_date = end_date - timedelta(days=new_period)
    elif freq=="h":
        new_period = (int(math.ceil(period/6))+12)*24
        start_date = end_date - timedelta(hours=new_period)
        interval="1h"
    elif freq=="m":
        start_date = end_date - timedelta(minutes=new_period)
        interval="1m"

    # Format dates as YYYY-MM-DD strings, which yfinance expects
    end_date_str = end_date.strftime('%Y-%m-%d')
    start_date_str = start_date.strftime('%Y-%m-%d')

    print(f"Fetching data for {ticker_symbol} from {start_date_str} to {end_date_str} with interval {interval}...")

    try:
        # Download stock data
        stock_data = yf.download(ticker_symbol, start=start_date_str, end=end_date_str, interval=interval, progress=False)

        if stock_data.empty:
            print(f"No data found for ticker {ticker_symbol} for the last 10 years.")
            return False

        # Add a Ticker column for easier identification if concatenating multiple CSVs later
        #stock_data['Ticker'] = ticker_symbol

        # Save the DataFrame to a CSV file
        print(f"Returning Datatype: {type(stock_data)}")
        print(f"Before Size: {len(stock_data)}")
        return stock_data.tail(period)["Close"].to_numpy().flatten() if return_sequence else stock_data.tail(period)

    except Exception as e:
        print(f"An error occurred while fetching or saving data for {ticker_symbol}: {e}")
        return None
    
if __name__=="__main__":
    ticker="MSFT"
    #print(fetch_and_save_stock_data_10_years(ticker))
    result=fetch_prev_stock_data(ticker, period=60,freq="h",return_sequence=False)
    print(result)
    print(len(result))