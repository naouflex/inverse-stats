import traceback
import requests
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from tools.database import save_table, get_table, update_table,remove_duplicates,drop_table,table_exists
from dotenv import load_dotenv
import os
import logging

logger = logging.getLogger(__name__)
MAX_THREADS = 10 

load_dotenv()

def fetch_json(url):
    return requests.get(url).json()

def fetch_token_data(chain_slug, contract_address):
    url = f"https://coins.llama.fi/prices/first/{chain_slug}:{contract_address}"
    return fetch_json(url)

def fetch_chart_data(chain_slug, contract_address, start_timestamp, days):
    url = f"https://coins.llama.fi/chart/{chain_slug}:{contract_address}?start={start_timestamp}&span={days}&period=1d"
    return fetch_json(url)

def fetch_and_update_data(token_info, data):
    try:
        chain_slug, contract_address = token_info['chain_slug'], token_info['contract_address']

        # Determine the start_timestamp based on the token_info or fetch from the API
        start_timestamp = token_info.get('timestamp')

        if not start_timestamp:
            token_data = fetch_token_data(chain_slug, contract_address)
            start_timestamp = int(token_data["coins"][f"{chain_slug}:{contract_address}"]['timestamp'])

        #set hour , minute and second to 0 and make sure this is utc
        start_timestamp = int(datetime.utcfromtimestamp(start_timestamp).replace(hour=0, minute=0, second=0).timestamp())
        end_timestamp = int(datetime.utcnow().replace(hour=0, minute=0, second=0).timestamp())
        
        # We use days_since_start + 1 because we want to include both the start date and the end date
        days_since_start = int(( datetime.utcfromtimestamp(end_timestamp) - datetime.utcfromtimestamp(start_timestamp)).days) + 2
        
        # Update token_info with the correct range
        token_info.update({'timestamp': start_timestamp, 'days': days_since_start})

        chart_data = fetch_chart_data(chain_slug, contract_address, start_timestamp, days_since_start)
        data['coins'].update(chart_data['coins'])

    except Exception as e:
        print(f"Error in fetch_and_update_data for {token_info} : {traceback.print_exc()}")
        print("Please check if the token is still valid on defillama")
        pass

def fetch_current_prices_from_tokens(token_address_list):
    tokens = ','.join([f"{info['chain_slug']}:{info['contract_address']}" for info in token_address_list])
    url = f"https://coins.llama.fi/prices/current/{tokens}?searchWidth=4h"
    
    return fetch_json(url)

def fetch_and_save_current_data(token_info, data):
    try:
        current_prices_data = fetch_current_prices_from_tokens(token_info)
        
        for key, value in current_prices_data['coins'].items():
            chain_slug, contract_address = key.split(':')
            
            coin_data = {
                'prices': [[int(value['timestamp']), value['price']]],
                'confidence': value.get('confidence', None),
                'symbol': value.get('symbol', None),
                'decimals': value.get('decimals', None)
            }
            
            data['coins'][f"{chain_slug}:{contract_address}"] = coin_data

    except Exception as e:
        print(traceback.print_exc())

def create_history():
    try:
        start_time = datetime.now()

        db_url = os.getenv('PROD_DB')
        table_name = 'defillama_prices'

    
        # Fetch token address list
        url = "https://app.inverse.watch/api/queries/480/results.json?api_key=JY9REfUM3L7Ietj76qmQ2wFioz7k6GdCL6YqRxHG"
        token_address_list = fetch_json(url)["query_result"]["data"]["rows"]

        data = {"coins": {}}

        # Using ThreadPoolExecutor to fetch data concurrently
        with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            for token_info in token_address_list:
                executor.submit(fetch_and_update_data, token_info, data)

        # Data manipulation using Pandas
        df = pd.DataFrame(data)
        df.reset_index(level=0, inplace=True)
        df.rename(columns={"index": "chain:token_address"}, inplace=True)
        df[['chain', 'token_address']] = df['chain:token_address'].str.split(':', expand=True)

        df = pd.concat([df.drop(['coins', 'chain:token_address'], axis=1), df['coins'].apply(pd.Series)], axis=1)
        df = df.explode('prices')
        df = pd.concat([df.drop(['prices'], axis=1), df['prices'].apply(pd.Series)], axis=1)

        # add chain_id column
        chain_id_map = {'ethereum': 1, 'bsc': 56, 'polygon': 137, 'fantom': 250, 'optimism': 10, 'arbitrum': 42161, 'avax': 43114}
        df['chain_id'] = df['chain'].map(chain_id_map)

        # Convert timestamp column to datetime object
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')

        # Round datetime to midnight
        df['timestamp'] = df['timestamp'].dt.normalize()

        # Convert datetime back to Unix timestamp
        df['timestamp'] = df['timestamp'].astype(int) // 10 ** 9

        df['last_updated'] = datetime.now()

        # Filter out any keys not in DataFrame columns
        valid_keys = {k: v for k, v in {
            'timestamp': 'Int64', 
            'chain': 'string', 
            'chain_id': 'Int64', 
            'token_address': 'string',
            'price': 'float64',
            'confidence': 'float64',
            'symbol': 'string',
            'decimals': 'Int64',
            'last_updated': 'datetime64[ns]'
        }.items() if k in df.columns}
        
        # Make sure all columns are in the proper format
        df = df.astype(valid_keys)

        if table_exists(db_url, table_name):
            drop_table(db_url, table_name)
        
        save_table(db_url, table_name, df)

        remove_duplicates(db_url, table_name, ['timestamp', 'chain_id', 'token_address'], 'last_updated')

        logger.info(f"Total time: {datetime.now() - start_time}")

    except Exception:
        logger.error(f"Error in creating historical price table : {traceback.print_exc()}")
        pass

def create_current():
    try:
        start_time = datetime.now()
        db_url = os.getenv('PROD_DB')
        table_name = 'defillama_prices_current'

        # Fetch token address list
        url = "https://app.inverse.watch/api/queries/480/results.json?api_key=JY9REfUM3L7Ietj76qmQ2wFioz7k6GdCL6YqRxHG"
        token_address_list = fetch_json(url)["query_result"]["data"]["rows"]

        data = {"coins": {}}

        # Fetch current prices for all tokens using the new API
        fetch_and_save_current_data(token_address_list, data)

        # Rest of the data processing is the same as before
        df = pd.DataFrame(data)
        
        df.reset_index(level=0, inplace=True)
        df.rename(columns={"index": "chain:token_address"}, inplace=True)
        df[['chain', 'token_address']] = df['chain:token_address'].str.split(':', expand=True)
        df = pd.concat([df.drop(['coins', 'chain:token_address'], axis=1), df['coins'].apply(pd.Series)], axis=1)
        df = df.explode('prices')
        
        df = pd.concat([df.drop(['prices'], axis=1), df['prices'].apply(pd.Series)], axis=1)
        # rename 0 and 1 to timestamp and price
        df.rename(columns={0: 'timestamp', 1: 'price'}, inplace=True)

        # add chain_id column
        chain_id_map = {'ethereum': 1, 'bsc': 56, 'polygon': 137, 'fantom': 250, 'optimism': 10, 'arbitrum': 42161, 'avax': 43114}
        df['chain_id'] = df['chain'].map(chain_id_map)

        df['last_updated'] = datetime.now()

        # Filter out any keys not in DataFrame columns
        valid_keys = {k: v for k, v in {
            'timestamp': 'Int64', 
            'chain': 'string', 
            'chain_id': 'Int64', 
            'token_address': 'string',
            'price': 'float64',
            'confidence': 'float64',
            'symbol': 'string',
            'decimals': 'Int64',
            'last_updated': 'datetime64[ns]'
        }.items() if k in df.columns}
        
        # Make sure all columns are in the proper format
        df = df.astype(valid_keys)

        # Save data to database if table exists, otherwise create table
        if table_exists(db_url, table_name):
            drop_table(db_url, table_name)

        save_table(db_url, table_name, df)

        end_time = datetime.now()
        logger.info(f"Total time: {end_time - start_time}")
        
    except Exception as e:
        logger.error(f"Error in creating current price table : {traceback.print_exc()}")
        pass