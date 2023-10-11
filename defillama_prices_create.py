import traceback
import requests
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from tools.database import save_table, get_table, update_table
from dotenv import load_dotenv
import os
import logging
from dotenv import load_dotenv

logger = logging.getLogger(__name__)
MAX_THREADS = 10  # Maximum number of threads

load_dotenv()

def fetch_json(url):
    return requests.get(url).json()

def fetch_token_data(chain_slug, contract_address):
    url = f"https://coins.llama.fi/prices/first/{chain_slug}:{contract_address}"
    return fetch_json(url)

def fetch_chart_data(chain_slug, contract_address, timestamp, days):
    url = f"https://coins.llama.fi/chart/{chain_slug}:{contract_address}?start={timestamp}&span={days}&period=1d"
    return fetch_json(url)

def calculate_days_since_start(start_timestamp):
    return int((datetime.now() - datetime.fromtimestamp(start_timestamp)).days + 1)

def fetch_and_update_data(token_info, data):
    chain_slug, contract_address = token_info['chain_slug'], token_info['contract_address']

    token_data = fetch_token_data(chain_slug, contract_address)
    start_timestamp = int(token_data["coins"][f"{chain_slug}:{contract_address}"]['timestamp'])
    start_timestamp = start_timestamp - (start_timestamp % 86400) + 86340

    days_since_start = calculate_days_since_start(start_timestamp)
    token_info.update({'timestamp': start_timestamp, 'days': days_since_start})

    chart_data = fetch_chart_data(chain_slug, contract_address, start_timestamp, days_since_start)
    data['coins'].update(chart_data['coins'])
    print(f"Fetched data for {chain_slug}:{contract_address}")

def create_price_table(db_url, table_name):
    try:
        # Fetch token address list
        url = "https://app.inverse.watch/api/queries/480/results.json?api_key=JY9REfUM3L7Ietj76qmQ2wFioz7k6GdCL6YqRxHG"
        token_address_list = fetch_json(url)["query_result"]["data"]["rows"]

        data = {"coins": {}}

        # Using ThreadPoolExecutor to fetch data concurrently
        with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            executor.map(fetch_and_update_data, token_address_list, [data]*len(token_address_list))

        # Data manipulation using Pandas
        df = pd.DataFrame(data)
        df.reset_index(level=0, inplace=True)
        df.rename(columns={"index": "chain:token_address"}, inplace=True)
        df[['chain', 'token_address']] = df['chain:token_address'].str.split(':', expand=True)

        df = pd.concat([df.drop(['coins', 'chain:token_address'], axis=1), df['coins'].apply(pd.Series)], axis=1)
        df = df.explode('prices')
        df = pd.concat([df.drop(['prices'], axis=1), df['prices'].apply(pd.Series)], axis=1)

        chain_id_map = {'ethereum': 1, 'bsc': 56, 'polygon': 137, 'fantom': 250, 'optimism': 10, 'arbitrum': 42161, 'avax': 43114}
        df['chain_id'] = df['chain'].map(chain_id_map)

        df = df.sort_values('timestamp').drop_duplicates(['timestamp', 'token_address'], keep='last')
        
        # Filter out any keys not in DataFrame columns
        valid_keys = {k: v for k, v in {
            'timestamp': 'Int64', 
            'chain': 'string', 
            'chain_id': 'Int64', 
            'token_address': 'string',
            'price': 'float64',
            'confidence': 'float64',
            'symbol': 'string',
            'decimals': 'Int64'
        }.items() if k in df.columns}
        
        # Make sure all columns are in the proper format
        df = df.astype(valid_keys)

        # Save data to database
        save_table(db_url, table_name, df)
    
    except Exception as e:
        logger.error(f"Error in creating price table : {traceback.print_exc()}")


def run():
    start_time = datetime.now()
    db_url = os.getenv('PROD_DB')
    create_price_table(db_url, 'defillama_prices')
    end_time = datetime.now()
    logger.info(f"Total time: {end_time - start_time}")
