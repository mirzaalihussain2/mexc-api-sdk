import concurrent.futures
import time
from datetime import datetime, timezone, timedelta
from mexc_sdk import Spot
from dotenv import load_dotenv
import os
from helpers import process_pair
import pprint

# Load environment variables from .env file
load_dotenv()

# Retrieve API key and secret from environment variables
api_key = os.getenv('API_KEY')
api_secret = os.getenv('API_SECRET')

# Initialize the Spot client with your API key and secret
spot = Spot(api_key=api_key, api_secret=api_secret)

# Fetch exchange information
exchange_info = spot.exchange_info()

# Extract the list of trading pairs
trading_pairs = [symbol['symbol'] for symbol in exchange_info['symbols']] # 3064 trading pairs
usdt_pairs = [pair for pair in trading_pairs if pair.endswith('USDT')] # 2937 USDT pairs

# Start with the current month
current_date = datetime.now()
too_far_back = datetime.now() - timedelta(days=4 * 365) # 4 years

def process_pairs_concurrently(spot, pairs, current_date, too_far_back):
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = {executor.submit(process_pair, spot, pair, current_date, too_far_back): pair for pair in pairs}
        results = []
        for future in concurrent.futures.as_completed(futures):
            try:
                result = future.result()
                # Check if all required fields are present before appending
                if (result["earliest trading month"] and 
                    result["latest trading date"] and 
                    result["earliest trading date"]):
                    results.append(result)
            except Exception as e:
                print(f"Error processing pair {futures[future]}: {e}")
    return results

# Process pairs and write results to CSV
results = process_pairs_concurrently(spot, usdt_pairs, current_date, too_far_back)

# Write results to CSV
import csv
with open('data/symbol_info.csv', 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=['symbol', 'earliest trading month', 'earliest trading date', 'latest trading date'])
    writer.writeheader()
    writer.writerows(results)
