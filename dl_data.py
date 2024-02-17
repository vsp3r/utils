import os
import requests
import argparse
import json
from decompress import decompress_files, compress_files

def download_market_data(date, coin, decomp=False):
    base_url = 'https://hyperliquid-archive.s3.amazonaws.com/market_data'
    base_local_path = os.path.join('data', date, coin)

    # Create date and coin specific folder
    if not os.path.exists(base_local_path):
        os.makedirs(base_local_path)

    for hour in range(24):
        file_name = f"{hour}.lz4"
        url = f"{base_url}/{date}/{hour}/l2Book/{coin}.lz4"
        local_file_path = os.path.join(base_local_path, file_name)

        # If file already exists, skip
        if os.path.exists(local_file_path):
            print(f"File {file_name} already exists. Skipping...")
            continue

        response = requests.get(url)

        if response.status_code == 200:
            with open(local_file_path, 'wb') as file:
                file.write(response.content)
            print(f"Downloaded {file_name} successfully.")
        else:
            print(f"Failed to download {file_name}. Status code: {response.status_code}")
    if decomp:
        decompress_files(date, coin, hours=24) 

def process_tasks_from_config(decompress=False, compress=False):
    with open('batch.json', 'r') as file:
        config = json.load(file)

    for date in config['dates']:
        for coin in config['coins']:
            if not decompress and not compress:
                download_market_data(date, coin)
            if decompress:
                decompress_files(date, coin)
            if compress:
                compress_files(date, coin)

def group_data(date, coin, delete_flag=False):
    base_path = os.path.join('data', date, coin)
    decompress_files(date, coin)

    aggregated_f = os.path.join('data', date, f"{coin}_book.dat")
    with open(aggregated_f, 'wb') as output:
        for hour in range(24):
            input_path = os.path.join(base_path, f"{hour}.dat")
            if os.path.exists(input_path):
                with open(input_path, 'rb') as input:
                    output.write(input.read())
            else:
                print(f"File not found: {input_path}")

    if delete_flag:
        os.remove(base_path)



def parse_args():
    parser = argparse.ArgumentParser(description='Manage market data files.')
    parser.add_argument('-d', '--decompress', action='store_true', help='Decompress the downloaded files')
    parser.add_argument('-c', '--compress', action='store_true', help='Compress the decompressed files')
    parser.add_argument('-b', '--batch', action='store_true', help='Process tasks from config file')
    parser.add_argument('--date', type=str, help='Date in YYYYMMDD format', default='')
    parser.add_argument('--coin', type=str, help='Coin symbol', default='')
    return parser.parse_args()

def main():
    args = parse_args()

    if args.batch:
        process_tasks_from_config(decompress=args.decompress, compress=args.compress)
    else:
        if not args.decompress and not args.compress:
            if args.date and args.coin:
                download_market_data(args.date, args.coin)
        if args.decompress and args.date and args.coin:
            decompress_files(args.date, args.coin)
        if args.compress and args.date and args.coin:
            compress_files(args.date, args.coin)

if __name__ == '__main__':
    main()
