import os
import numpy as np
import pandas as pd
from fyers_apiv3 import fyersModel
from datetime import datetime, timedelta
import time
import threading
import configparser
from trade_timer import get_trading_times

# Function to read config file
def read_config(file_path):
    config = configparser.ConfigParser()
    config.read(file_path)

    data = []
    for section in config.sections():
        symbol_data = {
            "symbol": config[section]["symbol"],
            "fut_symbol": config[section]["fut_symbol"],
            "lbk_period": int(config[section]["lbk_period"]),
            "stoploss": int(config[section]["stoploss"]),
            "takeprofit": int(config[section]["takeprofit"]),
        }
        data.append(symbol_data)

    return data

# Function to fetch historical data
def fetch_historical_data(symbol, interval, start_date, end_date):
    fyers = fyersModel.FyersModel(client_id=app_id, token=access_token)

    data = {
        "symbol": symbol,
        "resolution": interval,
        "date_format": "0",
        "range_from": start_date,
        "range_to": end_date,
        "cont_flag": "1"
    }

    response = fyers.history(data)
    if 'candles' in response:
        data = response['candles']
        df = pd.DataFrame(data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
        df['timestamp'] = df['timestamp'] + pd.Timedelta(hours=5, minutes=30)
        return df
    else:
        print("No data found in response:", response)
        return pd.DataFrame()

# Function to fetch orderbook
def fetch_orderbook(symbol):
    fyers = fyersModel.FyersModel(client_id=app_id, token=access_token)
    data = {"symbol": symbol}
    response = fyers.orderbook(data)
    return response

# Function to calculate Exponential Moving Average (EMA)    
def calculate_ema(series, span):
    alpha = 2 / (span + 1)
    ema = [series.iloc[0]]  # Starting point

    for price in series[1:]:
        ema.append((price - ema[-1]) * alpha + ema[-1])

    return pd.Series(ema, index=series.index)

# Function to create signals based on the strategy
def create_signal(df, lbk_period):
    df['ema_high'] = round((calculate_ema(df['high'], lbk_period)), 2)
    df['ema_low'] = round((calculate_ema(df['low'], lbk_period)), 2)
    df['signal'] = 0

    if df['close'].iloc[-1] > df['ema_high'].iloc[-1]:
        df.loc[df.index[-1], 'signal'] = 1

    elif df['close'].iloc[-1] < df['ema_low'].iloc[-1]:
        df.loc[df.index[-1], 'signal'] = -1
        
    return df


# Function to fetch real-time price
def get_realtime_price(symbol):
    fyers = fyersModel.FyersModel(client_id=app_id, token=access_token)
    data = {"symbols": symbol}
    response = fyers.quotes(data)
    if 'd' in response and len(response['d']) > 0:
        return response['d'][0]['v']['lp']  # 'lp' stands for last traded price
    else:
        print('Error fetching real-time price:', response)
        return None


# Function to place an order
def place_order(symbol, qty, side, productType, orderType, price):

    fyers = fyersModel.FyersModel(client_id=app_id, token=access_token)
    response = fyers.place_orders(
        token=access_token, 
        data={
            "symbol": symbol, 
            "qty": qty, 
            "side": side, 
            "type": productType, 
            "orderType": orderType, 
            "price": price
        }
    )
    print(response)
    return response

# Function to record a trade in the tradebook
def record_trade(symbol, date, time, signal, price, lot, reason=''):
    global tradebook
    new_trade = pd.DataFrame([{
        'symbol': symbol,
        'date': date,
        'time': time,
        'signal': signal,
        'price': price,
        'lot': lot,
        'reason': reason
    }])
    
    if not new_trade.empty:  # Check if new_trade DataFrame is not empty
        if tradebook.empty:
            tradebook = new_trade  # If tradebook is empty, assign new_trade directly
        else:
            tradebook = pd.concat([tradebook, new_trade], ignore_index=True)
        tradebook.to_csv(tradebook_path, index=False)
    else:
        print("Warning: new_trade DataFrame is empty. Trade record not added.")

# Function to implement stoploss 
def strict_stoploss():
    try:
        now = datetime.now()
        open_positions = pd.read_csv("open_position.csv")
        to_remove = []

        for index, row in open_positions.iterrows():
            fut_symbol = row['fut_symbol']
            signal = row['signal']
            stoploss_value = row['stoploss']
            price = get_realtime_price(fut_symbol)

            if signal == "BUY":
                entry_price = row['price']
                stoploss_price = entry_price - stoploss_value
                if price <= stoploss_price:
                    print("my stoploss price is ",stoploss_price)
                    print(f"Stoploss hit for {fut_symbol}. Selling at {price}")
                    record_trade(fut_symbol, now.strftime('%Y-%m-%d'), now.strftime('%H:%M:%S'), 'SELL', price, 1, reason='stoploss')
                    # place_order(fut_symbol, 1, "SELL", "INTRADAY", "LIMIT", price)
                    to_remove.append(fut_symbol)

            elif signal == "SELL":
                entry_price = row['price']
                stoploss_price = entry_price + stoploss_value
                if price >= stoploss_price:
                    print("my stoploss price is ",stoploss_price)
                    print(f"Stoploss hit for {fut_symbol}. Buying at {price}")
                    record_trade(fut_symbol, now.strftime('%Y-%m-%d'), now.strftime('%H:%M:%S'), 'BUY', price, 1, reason='stoploss')
                    # place_order(fut_symbol, 1, "BUY", "INTRADAY", "LIMIT", price)
                    to_remove.append(fut_symbol)

        # Remove positions whose stoploss has been hit
        if to_remove:
            open_positions = open_positions[~open_positions['fut_symbol'].isin(to_remove)]
            open_positions.to_csv("open_position.csv", index=False)

    except FileNotFoundError:
        print("No open positions found. Skipping stoploss check.")


# Function to implement takeprofit
def strict_takeprofit():
    try:
        now = datetime.now()
        open_positions = pd.read_csv("open_position.csv")
        to_remove = []

        for index, row in open_positions.iterrows():
            fut_symbol = row['fut_symbol']
            signal = row['signal']
            takeprofit_value = row['takeprofit']
            price = get_realtime_price(fut_symbol)

            if signal == "BUY":
                entry_price = row['price']
                takeprofit_price = entry_price + takeprofit_value
                if price >= takeprofit_price:
                    print("my takeprofit price is ",takeprofit_price)
                    print(f"Takeprofit hit for {fut_symbol}. Selling at {price}")
                    record_trade(fut_symbol, now.strftime('%Y-%m-%d'), now.strftime('%H:%M:%S'), 'SELL', price, 1, reason='takeprofit')
                    # place_order(fut_symbol, 1, "SELL", "INTRADAY", "LIMIT", price)
                    to_remove.append(fut_symbol)

            elif signal == "SELL":
                entry_price = row['price']
                takeprofit_price = entry_price - takeprofit_value
                if price <= takeprofit_price:
                    print("my takeprofit price is ",takeprofit_price)
                    print(f"Takeprofit hit for {fut_symbol}. Buying at {price}")
                    record_trade(fut_symbol, now.strftime('%Y-%m-%d'), now.strftime('%H:%M:%S'), 'BUY', price, 1, reason='takeprofit')
                    # place_order(fut_symbol, 1, "BUY", "INTRADAY", "LIMIT", price)
                    to_remove.append(fut_symbol)

        # Remove positions whose takeprofit has been hit
        if to_remove:
            open_positions = open_positions[~open_positions['fut_symbol'].isin(to_remove)]
            open_positions.to_csv("open_position.csv", index=False)

    except FileNotFoundError:
        print("No open positions found. Skipping takeprofit check.")


# Read config file
config_file_path = 'config.cfg'
config_data = read_config(config_file_path)

# 15-minute interval for historical data
interval = "15"     
base_dir = r"C:\Users\arushi\Desktop\My work\fish_strategy\fyers_live_code\multifish_code"

app_id_path = os.path.join(base_dir, "fyers_appid.txt")
access_token_path = os.path.join(base_dir, "fyers_token.txt")
tradebook_path = os.path.join(base_dir, "tradebook.csv")

# Ensure the file paths are correct
if not os.path.isfile(app_id_path):
    raise FileNotFoundError(f"File not found: {app_id_path}")
if not os.path.isfile(access_token_path):
    raise FileNotFoundError(f"File not found: {access_token_path}")

app_id = open(app_id_path, 'r').read().strip()
access_token = open(access_token_path, 'r').read().strip()

# Initialize or load tradebook
try:
    tradebook = pd.read_csv(tradebook_path)
except FileNotFoundError:
    tradebook = pd.DataFrame(columns=['symbol', 'date', 'time', 'signal', 'price', 'lot', 'reason'])

# Initialize a dictionary to track if an interval has been processed
processed_intervals = {}

# Set the end time for the strategy (3:30 PM)
end_time = datetime.now().replace(hour=15, minute=30, second=0, microsecond=0)

# Read trading times from the file
file_path = 'trading_times.txt'
trading_times = get_trading_times(file_path)

# Function to check if the current time matches any of the trading times
def matching_time():
    current_time = datetime.now().time()
    for time in trading_times:
        # If the current time falls within the interval and hasn't been processed
        if current_time >= time.time() and current_time < (time + timedelta(minutes=15)).time():
            interval_str = time.strftime("%H:%M")
            if interval_str not in processed_intervals:
                processed_intervals[interval_str] = True
                return True
    return False
# Load previous day's positions if the file exists
try:
    prev_positions = pd.read_csv("open_position.csv")
    prev_positions.drop_duplicates(subset='fut_symbol', keep='first', inplace=True)
    prev_positions.set_index('fut_symbol', inplace=True)
    last_signals = prev_positions.to_dict(orient='index')
except FileNotFoundError:
    last_signals = {}


# Main loop to run the strategy at the start of every 15-minute interval
while datetime.now() < end_time:
    if matching_time():
        try:
            open_positions = pd.read_csv("open_position.csv")
        except FileNotFoundError:
            open_positions = pd.DataFrame(columns=['symbol', 'fut_symbol', 'signal', 'lot_size', 'price', 'stoploss', 'takeprofit'])

        # Fetch and process data for each symbol
        for item in config_data:
            symbol = item["symbol"]
            fut_symbol = item["fut_symbol"]
            lbk_period = item["lbk_period"]
            stoploss = item["stoploss"]
            takeprofit = item["takeprofit"]

            # Fetch historical data for the last 100 days
            start_date = int((datetime.now() - timedelta(days=100)).timestamp())
            end_date = int(datetime.now().timestamp())
            df = fetch_historical_data(symbol, interval, start_date, end_date)
            data = create_signal(df, lbk_period)

            latest_signal = data['signal'].iloc[-1]
            side = "BUY" if latest_signal == 1 else "SELL" if latest_signal == -1 else "NO SIGNAL"

            # Check if we already have the same position in open_position.csv
            current_position = open_positions[open_positions['fut_symbol'] == fut_symbol]

            if not current_position.empty:
                current_signal = current_position['signal'].values[0]
                current_lot_size = current_position['lot_size'].values[0]

                if current_signal == side:
                    # If the signal is the same as the previous one, keep the lot size as it is 
                    print(f"Signal for {symbol} is same as before. No new order placed.")
                    orderbook = fetch_orderbook(fut_symbol)
                    #print(orderbook)
                    continue  # Skip placing an order if signal is the same
                else:
                    # If the signal is different, set lot size to 2
                    lot = 2
            else:
                # Start with 1 lot if no previous position
                lot = 1

            if side in ["BUY", "SELL"]:
                print(f"Signal for {symbol}: {side}")
                price = get_realtime_price(fut_symbol)
                print(f"Placing order for {fut_symbol} with lot size {lot} at {price}")
                record_trade(fut_symbol, datetime.now().strftime('%Y-%m-%d'), datetime.now().strftime('%H:%M:%S'), side, price, lot, reason='new order')
                # place_order(fut_symbol, lot, side, 'INTRADAY', 'LIMIT', price)
                # Prepare new position DataFrame
                new_position = {
                    'symbol': symbol,
                    'fut_symbol': fut_symbol,
                    'signal': side,
                    'lot_size': lot,
                    'price': price,
                    'stoploss': stoploss,
                    'takeprofit': takeprofit
                }
                new_position_df = pd.DataFrame([new_position])
                new_position_df = new_position_df.astype(open_positions.dtypes.to_dict())

                if current_position.empty:
                    # Add new position if it does not exist
                    open_positions = pd.concat([open_positions, new_position_df], ignore_index=True)
                else:
                    # Update existing position
                    open_positions.update(new_position_df)

                open_positions.to_csv("open_position.csv", index=False)
            else:
                print(f"No valid signal for {symbol}")

    # Check for stoploss and takeprofit
    strict_stoploss()
    strict_takeprofit()

    # Sleep for 30 seconds to avoid constant checking
    time.sleep(30)  

print("Strategy run completed. Market closed.")