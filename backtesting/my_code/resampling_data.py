import pandas as pd
import numpy as np 
import datetime as dt
import matplotlib.pyplot as plt

import pandas as pd

class DataProcessor:
    def __init__(self, input_data):
        self.input_data = input_data.copy()

    def read_csv_with_header(self):
        # Add a row above the existing DataFrame with the column names
        column_names = ['DATE', 'TIME', 'TICK_PRICE', 'COLUMN1', 'VOLUME']
        new_row = pd.DataFrame([['', '', '', '', '']], columns=column_names)
        
        # Concatenate the new row above the existing DataFrame
        df_with_header = pd.concat([new_row, self.input_data], ignore_index=True)
        
        return df_with_header

    def clean_data(self):
        # Remove rows with empty strings in 'DATE' or 'TIME' columns
        self.input_data = self.input_data[self.input_data['DATE'].astype(str).str.strip().astype(bool) & 
                                            self.input_data['TIME'].astype(str).str.strip().astype(bool)]

    def preprocess_data(self):
        self.clean_data()

        # Convert 'DATE' and 'TIME' columns to datetime and combine them into a single timestamp column
        self.input_data['TIMESTAMP'] = pd.to_datetime(self.input_data['DATE'].astype(str) + ' ' + self.input_data['TIME'], errors='coerce')

        # Drop rows with missing timestamps
        self.input_data.dropna(subset=['TIMESTAMP'], inplace=True)

        # Set 'TIMESTAMP' as the index
        self.input_data.set_index('TIMESTAMP', inplace=True)

    def resample_data(self, interval='5S'):
        # Resample the data to specified interval and aggregate OHLC values
        ohlc_data = self.input_data.resample(interval).agg({
            'TICK_PRICE': ['first', 'max', 'min', 'last']  # Open, High, Low, Close
        })

        # Flatten the multi-level column index
        ohlc_data.columns = ['open', 'high', 'low', 'close']

        # Drop rows with any missing values
        ohlc_data.dropna(inplace=True)

        # Reset index
        ohlc_data.reset_index(inplace=True)

        return ohlc_data
# IF YOU WANT TO USE THIS CLASS PLEASE USE BELOW CODE ACCORDINGLY 
#from resampling_data import DataProcessor

# Read the CSV file into a DataFrame
#input_data = pd.read_csv('BANKNIFTY24MARFUT.csv', header=None, names=['DATE', 'TIME', 'TICK_PRICE', 'COLUMN1', 'VOLUME'])

# Create an instance of the DataProcessor class
#processor = DataProcessor(input_data)

# Read CSV data with header
#df_with_header = processor.read_csv_with_header()

# Preprocess the data
#processor.preprocess_data()

# Resample the data to 5-second intervals(By default the interval is 5 seconds)
# If you want to change the interval you can pass the interval as a parameter to the resample_data method like 1T for 1 minute AND 1H for 1 hour
#ohlc_data = processor.resample_data()

# Print the resulting OHLC data
#print(ohlc_data)
