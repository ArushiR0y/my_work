import subprocess
import time

# Paths to your Python scripts
session_token_script = r"C:\Users\arushi\Desktop\My work\fish_strategy\fyers_live_code\multifish_code\session_token.py"
trade_script = r"C:\Users\arushi\Desktop\My work\fish_strategy\fyers_live_code\multifish_code\trade.py"

# Function to run session_token.py and obtain access token
def run_session_token():
    print("Running session_token.py...")
    subprocess.run(["python", session_token_script])
    print("Session token acquired.")

# Function to continuously run trade.py with the acquired access token
def run_trade():
    print("Running trade.py...")
    while True:
        try:
            subprocess.run(["python", trade_script])
        except KeyboardInterrupt:
            print("\nExiting trade.py...")
            break
        except Exception as e:
            print(f"Error in trade.py: {e}")
            time.sleep(60)  # Retry after 1 minute if there's an error

if __name__ == "__main__":
    # Run session_token.py to get the access token
    run_session_token()
    
    # Once session_token.py completes, run trade.py continuously
    run_trade()
