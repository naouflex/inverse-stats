import os
import logging
import traceback
import requests
import pandas as pd
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from dotenv import load_dotenv

from scripts.tools.database import (
    save_table, get_table, update_table, 
    remove_duplicates, drop_table, table_exists
)
from scripts.tools.constants import CHAIN_ID_MAP,PRICE_METHODOLOGY

# Constants and configurations
logger = logging.getLogger(__name__)
MAX_THREADS = 10
lock = threading.Lock()
load_dotenv()


def fetch_json(url):
    return requests.get(url).json()

def validate_keys(data):
    valid_keys = {
        'timestamp': 'Int64',
        'chain': 'string',
        'chain_id': 'Int64',
        'token_address': 'string',
        'price': 'float64',
        'confidence': 'float64',
        'symbol': 'string',
        'decimals': 'Int64',
        'last_updated': 'datetime64[ns]'
    }
    for col, new_type in valid_keys.items():
        if col in data.columns:
            try:
                data[col] = data[col].astype(new_type)
            except TypeError:
                logger.error(f"Failed to cast column {col} to {new_type}")

def fetch_token_data(chain_slug, contract_address):
    url = f"https://coins.llama.fi/prices/first/{chain_slug}:{contract_address}"
    return fetch_json(url)

def fetch_chart_data(chain_slug, contract_address, end_timestamp, days):
    url = f"https://coins.llama.fi/chart/{chain_slug}:{contract_address}?end={end_timestamp}&span={days}&period=1d"
    return fetch_json(url)

def fetch_and_update_data(token_info, data):
    try:
        chain_slug, contract_address = token_info['chain_slug'], token_info['contract_address']
        token_data = fetch_token_data(chain_slug, contract_address)
        
        # check if token_info['timestamp'] exists and prevent KeyError if it doesn't
        if 'timestamp' in token_info:
            start_timestamp = int(token_info['timestamp']) + 86400
        else:
            start_timestamp = int(token_data["coins"][f"{chain_slug}:{contract_address}"]['timestamp'])
            start_timestamp = int(datetime.utcfromtimestamp(start_timestamp).replace(hour=0, minute=0, second=0).timestamp())

        end_timestamp = int(datetime.utcnow().replace(hour=0, minute=0, second=0).timestamp())
        days_since_start = (datetime.utcfromtimestamp(end_timestamp) - datetime.utcfromtimestamp(start_timestamp)).days
        token_info.update({'timestamp': start_timestamp, 'days': days_since_start})
        chart_data = fetch_chart_data(chain_slug, contract_address, end_timestamp, days_since_start)

        with lock:
            data['coins'][f"{chain_slug}:{contract_address}"] = chart_data['coins'][f"{chain_slug}:{contract_address}"]

    except Exception:
        logger.error(f"Cannot find price data for {token_info}")
        logger.error(traceback.format_exc())

def fetch_current_prices_from_tokens(token_address_list):
    tokens = ','.join([f"{info['chain_slug']}:{info['contract_address']}" for info in token_address_list])
    url = f"https://coins.llama.fi/prices/current/{tokens}?searchWidth=4h"
    return fetch_json(url)

def fetch_current_data(token_info, data):
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
    except Exception:
        logger.error(traceback.format_exc())

def process_dataframe(data,current=True):
    df = pd.DataFrame(data)
    df.reset_index(level=0, inplace=True)
    df.rename(columns={"index": "chain:token_address"}, inplace=True)
    df[['chain', 'token_address']] = df['chain:token_address'].str.split(':', expand=True)
    df = pd.concat([df.drop(['coins', 'chain:token_address'], axis=1), df['coins'].apply(pd.Series)], axis=1)
    df = df.explode('prices')
    df = pd.concat([df.drop(['prices'], axis=1), df['prices'].apply(pd.Series)], axis=1)
    df.rename(columns={0: "timestamp", 1: "price"}, inplace=True)

    if current:
        # current unix timestamp
        df['timestamp'] = pd.to_datetime(datetime.now())
    else:
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
        df['timestamp'] = df['timestamp'].apply(lambda x: x + timedelta(days=1) if x.time() >= pd.Timestamp('12:00:00').time() else x)
    
    df['timestamp'] = df['timestamp'].dt.normalize()

    # convert timetime to int64 and divide by 10^9 to get unix timestamp
    df['timestamp'] = df['timestamp'].astype('int64') // 10**9
    
    df['chain_id'] = df['chain'].map(CHAIN_ID_MAP)
    df['last_updated'] = datetime.now()
    validate_keys(df)
    return df

def create_history(db_url, table_name):
    try:
        start_time = datetime.now()
        token_address_list = fetch_json(PRICE_METHODOLOGY)["query_result"]["data"]["rows"]
        data = {"coins": {}}

        with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            for token_info in token_address_list:
                executor.submit(fetch_and_update_data, token_info, data)
                logger.info(f"Finished fetching data for {token_info}")

        df = process_dataframe(data, current=False)

        if table_exists(db_url, table_name):
            drop_table(db_url, table_name)
        save_table(db_url, table_name,table_description, df)
        remove_duplicates(db_url, table_name, ['timestamp', 'chain_id', 'token_address'], 'last_updated')
        logger.info(f"Total time: {datetime.now() - start_time}")
    except Exception:
        logger.error(f"Error in creating historical price table : {traceback.format_exc()}")

def create_current(db_url, table_name):
    try:
        start_time = datetime.now()
        token_address_list = fetch_json(PRICE_METHODOLOGY)["query_result"]["data"]["rows"]
        data = {"coins": {}}
        fetch_current_data(token_address_list, data)

        df = process_dataframe(data, current=True)

        if table_exists(db_url, table_name):
            drop_table(db_url, table_name)
        save_table(db_url, table_name,table_description_current, df)
        logger.info(f"Total time: {datetime.now() - start_time}")
    except Exception:
        logger.error(f"Error in creating current price table : {traceback.format_exc()}")

def update_history(db_url, table_name):
    try:
        start_time = datetime.now()
        token_address_list = fetch_json(PRICE_METHODOLOGY)["query_result"]["data"]["rows"]
        data = {"coins": {}}

        with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            for token_info in token_address_list:
                executor.submit(fetch_and_update_data, token_info, data)
                logger.info(f"Finished fetching data for {token_info}")

        df = process_dataframe(data, current=False)

        if table_exists(db_url, table_name):
            drop_table(db_url, table_name)
        save_table(db_url, table_name,table_description, df)
        remove_duplicates(db_url, table_name, ['timestamp', 'chain_id', 'token_address'], 'last_updated')
        logger.info(f"Total time: {datetime.now() - start_time}")
    except Exception:
        logger.error(f"Error in updating historical price table : {traceback.format_exc()}")
