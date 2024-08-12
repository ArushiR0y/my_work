import os
import numpy as np
import pandas as pd
from fyers_apiv3 import fyersModel
from datetime import datetime, timedelta
import time
import threading
import json

# Initialize the base directory
base_dir = r"C:\Users\arushi\Desktop\My work\fish_strategy\fyers_live_code\fish_live_code"


# Define the path to the JSON file
json_file_path = os.path.join(base_dir, "contract.json")

# Read the data from the JSON file
with open(json_file_path, 'r') as json_file:
    contract_data = json.load(json_file)

# Extract values from the JSON data
symbol = contract_data['symbol']
fut_symbol = contract_data['fut_symbol']
lbk_period = contract_data['lbk_period']
stoploss_value = contract_data['stoploss']
takeprofit_value = contract_data['takeprofit']
# 15-minute interval for historical data
interval = "15"  

app_id_path = os.path.join(base_dir, "fyers_appid.txt")
access_token_path = os.path.join(base_dir, "fyers_token.txt")
csv_file_path = os.path.join(base_dir, "historical_data.csv")
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
    

    

# Function to calculate the time until the next 15-minute interval
def time_until_next_interval():
    now = datetime.now()
    next_interval = (now + timedelta(minutes=15 - now.minute % 15)).replace(second=0, microsecond=0)
    return (next_interval - now).total_seconds()




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
        print('buy signal')
    elif df['close'].iloc[-1] < df['ema_low'].iloc[-1]:
        df.loc[df.index[-1], 'signal'] = -1
        print('sell signal')

    return df   


# Function to execute a trade
def execute_trade(side, lot, reason):
    current_time = datetime.now()
    date_str = current_time.strftime('%Y-%m-%d')
    time_str = current_time.strftime('%H:%M:%S')
    price = get_realtime_price(fut_symbol)
    print("Future price =", price)
    if price is None:
        print('Failed to fetch real-time price, skipping trade.')
        return
    # Simulate placing an order
    # place_order(fut_symbol, lot, side, 'DELIVERY', 'LIMIT', price)
    record_trade(fut_symbol, date_str, time_str, side, price, lot, reason=reason)
    print(f"Trade executed: {side} with lot {lot} due to {reason}")


# Function to process data and place trades
def process_and_trade():
    # Fetch historical data for the last 100 days
    start_date = int((datetime.now() - timedelta(days=100)).timestamp())
    end_date = int(datetime.now().timestamp())
    df = fetch_historical_data(symbol, interval, start_date, end_date)

    if not df.empty:
        # Store the DataFrame to a CSV file, overwriting the previous data
        df.to_csv(csv_file_path, index=False)
        print("Updated DataFrame:")

        # Read the updated data and create a signal based on the strategy
        data = pd.read_csv(csv_file_path)
        data = create_signal(data, lbk_period)
        print(data.tail(2))

        # Get the latest signal
        latest_signal = data['signal'].iloc[-1]
        side = "BUY" if latest_signal == 1 else "SELL" if latest_signal == -1 else None

        if side:
            if tradebook.empty:
                execute_trade(side, 1, 'strategy signal')
            elif tradebook['reason'].iloc[-1] == 'strategy signal':
                previous_side = tradebook['signal'].iloc[-1]
                lot = tradebook['lot'].iloc[-1]

                if (side == 'BUY' and previous_side == 'SELL') or (side == 'SELL' and previous_side == 'BUY'):
                    new_lot = 2 if lot == 1 else lot
                    execute_trade(side, new_lot, 'strategy signal')
            elif tradebook['reason'].iloc[-1] in ['stoploss', 'takeprofit']:
                if tradebook['lot'].iloc[-1] == 1:
                    execute_trade(side, 1, 'strategy signal')

    else:
        print("No new data to update.")


# Main loop for continuous trading logic
def main_loop():
    while True:
        try:
            now = datetime.now()
            market_close_time = now.replace(hour=15, minute=30, second=0, microsecond=0)

            if now >= market_close_time:
                print("Market Closed..................")
                break

            # Process data and trade
            process_and_trade()  # Your function for trading logic

            # Calculate the time to sleep until the next 15-minute interval
            sleep_time = time_until_next_interval()
            print(f"Sleeping for {sleep_time} seconds")
            time.sleep(sleep_time)
        except Exception as e:
            print(f"An error occurred: {e}")
            break


# Function for pre-close updates
def pre_close_update():
    while True:
        now = datetime.now()
        market_close_time = now.replace(hour=15, minute=30, second=0, microsecond=0)
        pre_close_update_time = now.replace(hour=15, minute=29, second=0, microsecond=0)

        if now >= pre_close_update_time and now < market_close_time:
            print("Pre-close update at 03:29 PM")
            process_and_trade()
            break


# Function to implement stoploss logic
def stoploss(stoploss_value):
    while True:
        now = datetime.now()
        market_close_time = now.replace(hour=15, minute=30, second=0, microsecond=0)
        market_open_sl = now.replace(hour=9, minute=15, second=0, microsecond=0)

        if now >= market_open_sl and now < market_close_time:
            if tradebook.empty:
                print("Tradebook is empty, skipping stoploss check.")
            else:
                # Fetch real-time price
                price = get_realtime_price(fut_symbol)
                if price is None:
                    print('Failed to fetch real-time price, skipping stoploss update.')
                else:
                    # Check for stoploss condition
                    if tradebook['reason'].iloc[-1] == 'strategy signal':
                        if tradebook['signal'].iloc[-1] == 'BUY':
                            entry_price = tradebook['price'].iloc[-1]
                            stoploss_price = entry_price - stoploss_value
                            if price <= stoploss_price:
                                # Simulate placing an order
                                # place_order(fut_symbol, 1, 'SELL', 'DELIVERY', 'LIMIT', price)
                                record_trade(fut_symbol, now.strftime('%Y-%m-%d'), now.strftime('%H:%M:%S'), 'SELL', price, 1, reason='stoploss')
                                print("Stoploss executed: SELL")
                        elif tradebook['signal'].iloc[-1] == 'SELL':
                            entry_price = tradebook['price'].iloc[-1]
                            stoploss_price = entry_price + stoploss_value
                            if price >= stoploss_price:
                                # Simulate placing an order
                                # place_order(fut_symbol, 1, 'BUY', 'DELIVERY', 'LIMIT', price)
                                record_trade(fut_symbol, now.strftime('%Y-%m-%d'), now.strftime('%H:%M:%S'), 'BUY', price, 1, reason='stoploss')
                                print("Stoploss executed: BUY")
            time.sleep(60)  # Sleep for 1 minute before checking again

        else:
            break


# Function to implement takeprofit logic
def takeprofit(takeprofit_value):
    while True:
        now = datetime.now()
        market_close_time = now.replace(hour=15, minute=30, second=0, microsecond=0)
        market_open_tp = now.replace(hour=9, minute=15, second=0, microsecond=0)
        
        if now >= market_open_tp and now < market_close_time:
            if tradebook.empty:
                print("Tradebook is empty, Skipping takeprofit logic.")
            else:
                # Fetch real-time price
                price = get_realtime_price(fut_symbol)
                if price is None:
                    print('Failed to fetch real-time price, skipping takeprofit update.')
                else:
                    # Check for takeprofit condition
                    if tradebook['reason'].iloc[-1] == 'strategy signal':
                        if tradebook['signal'].iloc[-1] == 'BUY':
                            entry_price = tradebook['price'].iloc[-1]
                            takeprofit_price = entry_price + takeprofit_value
                            if price >= takeprofit_price:
                                # Simulate placing an order
                                #place_order(fut_symbol, 1, 'SELL', 'DELIVERY', 'LIMIT', price)
                                record_trade(fut_symbol, now.strftime('%Y-%m-%d'), now.strftime('%H:%M:%S'), 'SELL', price, 1, reason='takeprofit')
                                print("Takeprofit executed: SELL")
                        elif tradebook['signal'].iloc[-1] == 'SELL':
                            entry_price = tradebook['price'].iloc[-1]
                            takeprofit_price = entry_price - takeprofit_value
                            if price <= takeprofit_price:
                                # Simulate placing an order
                                #place_order(fut_symbol, 1, 'BUY', 'DELIVERY', 'LIMIT', price)
                                record_trade(fut_symbol, now.strftime('%Y-%m-%d'), now.strftime('%H:%M:%S'), 'BUY', price, 1, reason='takeprofit')
                                print("Takeprofit executed: BUY")
            time.sleep(60)  # Sleep for 1 minute before checking again
        else:
            break

if __name__ == "__main__":

     # Run main_loop in a separate thread
    main_loop_thread = threading.Thread(target=main_loop)
    main_loop_thread.start()

    # Run stoploss function in a separate thread with stoploss_value 
    stoploss_thread = threading.Thread(target=stoploss, args=(stoploss_value,))
    stoploss_thread.start()

    # Run takeprofit function in a separate thread with takeprofit_value
    takeprofit_thread = threading.Thread(target=takeprofit, args=(takeprofit_value,))
    takeprofit_thread.start()
    
    # Run pre_close_update in a separate thread
    pre_close_thread = threading.Thread(target=pre_close_update)
    pre_close_thread.start()

    # Wait for the threads to finish
    main_loop_thread.join()
    stoploss_thread.join()
    takeprofit_thread.join()
    pre_close_thread.join()  