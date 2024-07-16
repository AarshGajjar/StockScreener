import pandas as pd
from tvDatafeed import TvDatafeedLive, Interval
import ta
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import sys
import logging
import colorlog
import signal
import os

# Configuration
USERNAME = 'username'
PASSWORD = 'password'
INTERVAL = Interval.in_daily  # Change this to your desired timeframe
N_BARS = 365  # Number of bars for fetching historical data
MAX_WORKERS = 3  # Number of parallel threads
PADDING_LENGTH = 40  # Length for symbol padding
LOG_FILE = 'stock_screener.log'

# Determine the path to the Excel file
current_dir = os.path.dirname(os.path.abspath(__file__))
FILE_PATH = os.path.join(current_dir, 'MCAP28032024.xlsx')

# Initialize Logging
log_colors = {
    'DEBUG': 'white',
    'INFO': 'green',
    'WARNING': 'yellow',
    'ERROR': 'red',
    'CRITICAL': 'bold_red',
}

formatter = colorlog.ColoredFormatter(
    "%(log_color)s%(asctime)s - %(levelname)s - %(message)s",
    log_colors=log_colors
)

# Console handler
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

# File handler (override old logs)
file_handler = logging.FileHandler(LOG_FILE, mode='w')
file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

logger = logging.getLogger()
logger.addHandler(console_handler)
logger.addHandler(file_handler)
logger.setLevel(logging.INFO)

# Initialize TradingView data feed with credentials
logger.info("Attempting to initialize TvDatafeedLive with provided credentials...")
try:
    tvl = TvDatafeedLive(USERNAME, PASSWORD)
    logger.info("Successfully initialized TvDatafeedLive.")
except Exception as e:
    logger.error(f"Failed to initialize TvDatafeedLive: {e}")
    sys.exit(1)

# Global flag to check if the script should stop
stop_flag = threading.Event()

# List to keep track of SEIS instances
seis_list = []

def fetch_historical_data(symbol, exchange='NSE', interval=INTERVAL, n_bars=N_BARS):
    try:
        df = tvl.get_hist(symbol=symbol, exchange=exchange, interval=interval, n_bars=n_bars)
        if df is None or df.empty:
            logger.warning(f"No data available for symbol: {symbol}")
            return None
        return df
    except Exception as e:
        logger.error(f"Error fetching data for {symbol}: {e}")
        return None

def calculate_indicators(df):
    df['50DMA'] = df['close'].rolling(window=50).mean()
    df['200DMA'] = df['close'].rolling(window=200).mean()
    df['RSI'] = ta.momentum.RSIIndicator(df['close'], window=14).rsi()
    return df

def screen_stock(symbol, interval=INTERVAL, n_bars=N_BARS):
    logger.info(f"Screening stock: {symbol}")
    df = fetch_historical_data(symbol, interval=interval, n_bars=n_bars)
    if df is not None and len(df) >= 200:
        df = calculate_indicators(df)
        current_price = df['close'].iloc[-1]
        dma_50 = df['50DMA'].iloc[-1]
        dma_200 = df['200DMA'].iloc[-1]
        day_high = df['high'].iloc[-1]
        rsi = df['RSI'].iloc[-1]
        avg_volume = df['volume'].mean()
        current_volume = df['volume'].iloc[-1]

        # Check criteria
        if current_price > dma_50 and current_price > dma_200 and current_price >= (day_high * 0.999) and rsi < 70 and current_volume > avg_volume:
            logger.critical(f"Stock {symbol} meets the criteria")
            return symbol
    return None

def load_stock_symbols(file_path):
    try:
        stock_symbols = pd.read_excel(file_path)['Symbol'].tolist()
        logger.info("Stock symbols loaded successfully.")
        return stock_symbols
    except Exception as e:
        logger.error(f"Error loading stock symbols: {e}")
        sys.exit(1)

def main():
    selected_stocks = []
    stock_symbols = load_stock_symbols(FILE_PATH)
    
    if stock_symbols:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(screen_stock, symbol): symbol for symbol in stock_symbols}
            for future in as_completed(futures):
                try:
                    result = future.result()
                    if result:
                        selected_stocks.append(result)
                except Exception as e:
                    logger.error(f"Error screening stock: {e}")

        logger.info("Stocks with current price above 50 DMA and 200 DMA, 0.1% below day's high, and favorable volume and RSI conditions:")
        logger.info(selected_stocks)
    else:
        logger.warning("No stocks to screen.")

    for symbol in selected_stocks:
        seis = tvl.new_seis(symbol, 'NSE', INTERVAL)
        seis.new_consumer(lambda seis, data: consumer_func(seis, data))
        seis_list.append(seis)

def consumer_func(seis, data):
    logger.info(f"Received new data for {seis.symbol} on {seis.exchange} at {seis.interval.name}:")
    logger.info(data)

def signal_handler(sig, frame):
    logger.info("Stopping script...")
    stop_flag.set()
    for seis in seis_list:
        seis.del_consumer()
        seis.del_seis()
    sys.exit(0)

if __name__ == "__main__":
    # Register signal handler
    signal.signal(signal.SIGINT, signal_handler)

    # Run the main function
    main()
