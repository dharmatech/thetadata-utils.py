import pandas as pd
import os
import datetime
from pathlib import Path
from pandas.tseries.offsets import BDay

from thetadata_utils.get_all_options_trades import get_all_options_trades

from thetadata_api_v3.cached.caching import CACHE_DIR

class InvalidCacheError(Exception):
    """Raised when the cache file is invalid or corrupt."""
    pass

def get_cache_filepath(symbol: str) -> Path:
    cache_filename = f"{symbol}_all_options_trades.pkl"
    cache_filepath = CACHE_DIR / cache_filename
    return cache_filepath

def get_ny_now() -> pd.Timestamp:
    return pd.Timestamp.now(tz='America/New_York')

def get_today():
    ny_now = get_ny_now()
    return ny_now.date()

def is_market_open() -> bool:
    ny_now = pd.Timestamp.now(tz='America/New_York')
    is_weekday = ny_now.weekday() < 5
    market_open = is_weekday and (ny_now.time() >= datetime.time(9, 30)) and (ny_now.time() < datetime.time(16, 0))
    return market_open

def is_before_market_open() -> bool:
    ny_now = get_ny_now()
    is_weekday = ny_now.weekday() < 5
    before_open = is_weekday and (ny_now.time() < datetime.time(9, 30))
    return before_open

# ----------------------------------------------------------------------
def save_to_cache(df_all: pd.DataFrame):

    symbol = df_all['symbol'].iloc[0]

    ts_dates = pd.to_datetime(df_all['trade_timestamp'], errors='coerce').dt.date
    if is_market_open():
        print("Market is open, caching data only up to yesterday.")
        df_to_cache = df_all[ts_dates < get_today()]
    else:
        print("Market is closed, caching all data.")
        df_to_cache = df_all
    df_to_cache.to_pickle(get_cache_filepath(symbol))
# ----------------------------------------------------------------------
# symbol = 'GME'

def get_all_options_trades_cached(symbol: str) -> pd.DataFrame:
    cache_filepath = get_cache_filepath(symbol)
    today = get_today()

    if os.path.exists(cache_filepath):
        print(f"Loading cached data from {cache_filepath}")
        df_cached = pd.read_pickle(cache_filepath)

        # df_cached['trade_timestamp'].max()
                
        if (df_cached is None) or (not isinstance(df_cached, pd.DataFrame)) or df_cached.empty or ('trade_timestamp' not in df_cached.columns):
            raise InvalidCacheError(f"Cache is empty or missing trade_timestamp. Please delete the cache file at {cache_filepath} to continue.")
            
        # Parse timestamps without changing timezone; column is already Eastern Time
        df_cached['trade_timestamp'] = pd.to_datetime(df_cached['trade_timestamp'], errors='coerce')
        last_timestamp = df_cached['trade_timestamp'].max()

        if pd.isna(last_timestamp):
            raise InvalidCacheError(f"Cache has invalid timestamps. Please delete the cache file at {cache_filepath} to continue.")
            
        # "today" and market status already computed above in ET
        last_date = last_timestamp.date()

        print(f"Last cached trade date: {last_date}, Today: {today}, Market open: {is_market_open()}")

        if is_before_market_open():
            previous_trading_day = (pd.Timestamp(today) - BDay(1)).date()
            if last_date >= previous_trading_day:
                print("Cache is up-to-date as of previous trading day, and market is not open. Returning cached data.")
                return df_cached

        if last_date == today:
            print("Cache is already up-to-date for today. Returning cached data.")
            return df_cached
        
        start_date_new_fetch = (last_date + pd.Timedelta(days=1)).strftime('%Y-%m-%d')

        print(f"Cache found. Fetching new data from {start_date_new_fetch}...")
        df_new = get_all_options_trades(symbol, start_date=start_date_new_fetch)

        if not df_new.empty:
            df_all = pd.concat([df_cached, df_new], ignore_index=True).drop_duplicates()
            # print(f"Updating cache file at {cache_filepath}")
            print("Found new trades")
            save_to_cache(df_all)
            return df_all
        else :
            print("No new trades found.")
            df_all = df_cached
            return df_all
            
    else:
        print(f"No cached data found. Fetching all data for {symbol}...")
        df_all = get_all_options_trades(symbol)
        print(f"Saving fetched data to {cache_filepath}")
        save_to_cache(df_all)
        return df_all
        


# Since cached trades are only from completed trading days,
# `last_date` will always be before today if the market hasn't closed yet.

# If the market has closed and `last_date == today` then we're up-to-date and can return above.
#
# If `last_date == today` that means we have today's trades already.

# Consider renaming to:
# last_date_from_cache
