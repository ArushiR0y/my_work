import numpy as np
import pandas as pd
import datetime as dt
import matplotlib.pyplot as plt

class backtest_report:
    def __init__(self, data):
        self.data = data
        #print("Data:\n", self.data.head())# Check the first few rows of data

    def calculate_performance_metrics(self, turnover=10000000, brokerage=9500):
        # Calculate monthly and yearly P&L
        monthly_pnl = self.data.resample('M').sum()
        yearly_pnl = self.data.resample('Y').sum()
        
        # Calculate monthly and yearly P&L in percentage
        initial_capital = turnover
        monthly_pnl_percentage = (monthly_pnl / initial_capital) * 100
        yearly_pnl_percentage = (yearly_pnl / initial_capital) * 100

        # Total return
        total_return = (self.data.iloc[-1] - self.data.iloc[0]) / self.data.iloc[0]

        # Sharpe ratio (assuming risk-free rate of 0)
        daily_returns = self.data.pct_change()
        sharpe_ratio = (daily_returns.mean() / daily_returns.std()) * np.sqrt(252)

        # Drawdown percentage
        cumulative_max = self.data.cummax()
        drawdown = (self.data - cumulative_max) / cumulative_max
        max_drawdown_percentage = drawdown.min()

        # Hit ratio
        wins = (self.data > 0).sum()
        total_trades = (self.data != 0).sum()
        hit_ratio = wins / total_trades

        # Average profit and average loss
        average_profit = self.data[self.data > 0].mean()
        average_loss = self.data[self.data < 0].mean()
        if np.isnan(average_loss):
            average_loss = 0  # Set average loss to 0 if NaN

        # Expectancy
        if np.isnan(average_loss):
            expectancy = 0  # Set expectancy to 0 if average loss is NaN
        else:
            expectancy = (average_profit * hit_ratio) - (average_loss * (1 - hit_ratio))


        # Total trades
        total_trades = total_trades

        # Deduct brokerage
        brokerage_deducted = brokerage * (total_trades / 10000000)




        return {
            "Monthly P&L (%)": monthly_pnl_percentage,
            "Yearly P&L (%)": yearly_pnl_percentage,
            "Total Return": total_return,
            "Sharpe Ratio": sharpe_ratio,
            "Max Drawdown (%)": max_drawdown_percentage,
            "Hit Ratio": hit_ratio,
            "Average Profit": average_profit,
            "Average Loss": average_loss,
            "Expectancy": expectancy,
            "Total Trades": total_trades,
            "Brokerage Deducted": brokerage_deducted
        }
    
    def plot_equity_curve(self):
        equity = self.data.cumsum()
        print(equity)
        plt.figure(figsize=(10, 6))
        plt.plot(self.data.index, equity, label='Equity Curve', color='blue')
        plt.xlabel('Date')
        plt.ylabel('Equity')
        plt.title('Equity Curve')
        plt.legend()
        plt.show()

        
    def plot_drawdown(self):
        cumulative_max = self.data.cummax()
        drawdown = (self.data - cumulative_max) / cumulative_max

        plt.figure(figsize=(10, 6))
        plt.plot(self.data.index, drawdown, label='Drawdown', color='red')
        plt.xlabel('Date')
        plt.ylabel('Drawdown')
        plt.title('Drawdown over Time')
        plt.legend()
        plt.show()

#IF YOU WANT TO USE THIS CLASS PLEASE USE BELOW CODE ACCORDINGLY
#from Backtest_report_Generator import backtest_report
#import datetime as dt

# Reading the data and storing it into the dataframe
# df = pd.read_csv("Banknifty_Spot_data.csv")

# Convert the 'date' column to DateTime
# df['date'] = pd.to_datetime(df['date'])

# Set the 'date' column as the index
# df.set_index('date', inplace=True)

# Create an instance of backtest_report
# report = backtest_report(df['close']) 

# Calculate performance metrics
# metrics = report.calculate_performance_metrics()

# Plot equity curve
# report.plot_equity_curve()

# Plot drawdown curve
# report.plot_drawdown()

# Print the calculated metrics
# print(metrics)
           
           