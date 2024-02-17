import os
import requests
import json
import lz4.frame
from multiprocessing import Pool
import json
from datetime import datetime, timedelta
import time
import asyncio
import aiohttp

def update_dates_in_config(start_date, end_date, config_file='batch.json'):
    # Convert string dates to datetime objects
    start = datetime.strptime(start_date, "%Y%m%d")
    end = datetime.strptime(end_date, "%Y%m%d")

    # Generate list of dates
    date_list = []
    while start <= end:
        date_list.append(start.strftime("%Y%m%d"))
        start += timedelta(days=1)

    # Read the existing config file
    try:
        with open(config_file, 'r') as file:
            config = json.load(file)
    except (FileNotFoundError, json.decoder.JSONDecodeError):
        config = {}

    # Update the dates in the config
    config['dates'] = date_list

    # Write back to the config file
    with open(config_file, 'w') as file:
        json.dump(config, file, indent=4)

    print(f"Dates between {start_date} and {end_date} have been updated in {config_file}")

# Example usage

def download_asset(date):
    base_url = 'https://hyperliquid-archive.s3.amazonaws.com'
    meta = f'{base_url}/asset_ctxs/{date}.csv.lz4'
    base_local_path = os.path.join('data', date)

    # Create date and coin specific folder
    if not os.path.exists(base_local_path):
        os.makedirs(base_local_path)

    file_name = f"asset_ctxs.csv.lz4"
    local_file_path = os.path.join(base_local_path, file_name)

    response = requests.get(meta)

    if response.status_code == 200:
        with open(local_file_path, 'wb') as file:
            file.write(response.content)
        print(f"Downloaded {file_name} successfully.")
        decompress_asset(base_local_path)
    else:
        print(f"Failed to download {file_name}. Status code: {response.status_code}")

def decompress_asset(base_path):
    fname = 'asset_ctxs.csv'
    lz4_file_path = os.path.join(base_path, f"{fname}.lz4")
    decompressed_file_path = os.path.join(base_path, fname)

    try: 
        if os.path.exists(lz4_file_path):
            with open(lz4_file_path, 'rb') as lz4_file, open(decompressed_file_path, 'wb') as decompressed_file:
                decompressed_file.write(lz4.frame.decompress(lz4_file.read()))
            print(f"Decompressed {fname}.lz4 successfully.")
            os.remove(lz4_file_path)
        else:
            print(f"File {fname}.lz4 not found.")
    except Exception as e:
        print(f"An error occurred while decompressing {fname}.lz4: {e}")


def main():
    s = time.time()
    CONFIG_FILE = 'batch.json'
    update_dates_in_config("20231101", "20231130", config_file=CONFIG_FILE)
    # with open(CONFIG_FILE, 'r') as f:
    #     data = json.load(f)
    #     dates = data['dates']

    # Normal approach: 251 sec
    # for date in dates:
    #     download_asset(date)

    # multiprocessing pool approach: 154
    # with Pool() as pool:
    #     pool.map(download_asset, dates)

    # # Async approach:
    # # tasks = [download_asset(date) for date in dates]
    # # await asyncio.gather(*tasks)
  
    
    # end = time.time()
    # print(f"Completed in {end-s}")


if __name__ == '__main__':
    main()