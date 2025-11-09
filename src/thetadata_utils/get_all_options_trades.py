

# from datetime import datetime
import datetime

import pandas as pd

from tqdm import tqdm

from thetadata_api_v3.option_list_expirations           import option_list_expirations
from thetadata_api_v3.option_list_dates                 import option_list_dates
from thetadata_api_v3.cached.option_history_trade_quote import option_history_trade_quote

# symbol = 'GME'
# start_date = '2025-11-07'

def is_market_open() -> bool:
    ny_now = pd.Timestamp.now(tz='America/New_York')
    is_weekday = ny_now.weekday() < 5
    market_open = is_weekday and (ny_now.time() >= datetime.time(9, 30)) and (ny_now.time() < datetime.time(16, 0))
    return market_open

def is_after_market_close() -> bool:
    ny_now = pd.Timestamp.now(tz='America/New_York')
    is_weekday = ny_now.weekday() < 5
    after_close = is_weekday and (ny_now.time() >= datetime.time(16, 0))
    return after_close

def get_all_options_trades(symbol: str, start_date: str = None, end_date: str = None) -> pd.DataFrame:

    df_expirations = option_list_expirations(symbol=symbol)

    now = datetime.datetime.now()
    formatted_now = now.strftime('%Y-%m-%d')

    df_expirations_future = df_expirations[df_expirations['expiration'] > formatted_now]

    df_all = pd.DataFrame()

    # expiration = df_expirations_future['expiration'].iloc[0]
    # expiration = df_expirations_future['expiration'].iloc[0]

    # for expiration in df_expirations_future['expiration']:
    #     dates = option_list_dates(request_type='trade', symbol=symbol, expiration=expiration)
       
    #     print(dates['date'].iloc[-1])

    # option_list_dates(request_type='trade', symbol='GME', expiration='2025-11-14')
        

    # result = option_history_trade_quote(symbol=symbol, expiration=expiration, date='2025-11-07')


    for expiration in tqdm(df_expirations_future['expiration'], desc='Expirations'):

        dates = option_list_dates(request_type='trade', symbol=symbol, expiration=expiration)

        # get today

        today = datetime.datetime.now().strftime('%Y-%m-%d')

        # is_market_open = 

        # Add `today` to `dates` at the end
        if today not in dates['date'].values:
            if is_market_open():
                print("Market is open, adding today to dates to fetch trades.")
                dates = pd.concat([dates, pd.DataFrame({'date': [today]})], ignore_index=True)
            elif is_after_market_close():
                print("Market is closed, adding today to dates to fetch trades.")
                dates = pd.concat([dates, pd.DataFrame({'date': [today]})], ignore_index=True)

        if start_date:
            dates = dates[dates['date'] >= start_date]
        if end_date:
            dates = dates[dates['date'] <= end_date]
        
        for date in tqdm(dates['date'], desc=f'Getting trades for expiration {expiration}'):
            df = option_history_trade_quote(symbol=symbol, expiration=expiration, date=date)
            df_all = pd.concat([df_all, df])

    return df_all
    
