import requests
import pandas as pd
import logging
import os
from web3 import Web3
from datetime import datetime, timedelta
from dotenv import load_dotenv
from web3.middleware import geth_poa_middleware
from scripts.tools.chainkit import get_blocks_by_date_range
from scripts.tools.database import save_table, get_table, update_table, table_exists, drop_table

logger = logging.getLogger(__name__)
load_dotenv()

def get_rpc_table():
    try:
        web3_providers = requests.get(os.getenv('WEB3_PROVIDERS')).json()
        return pd.DataFrame(web3_providers['query_result']['data']['rows'])
    except Exception as e:
        logger.error(f"Cannot get rpc table: {e}")
        return

def process_chain_data(w3, chain_name, start_date=None, end_date=None):
    if start_date and end_date:
        return get_blocks_by_date_range(w3, start_date, end_date).rename(columns={'block_number': chain_name})
    else:
        return pd.DataFrame({
            'timestamp': datetime.now(),
            'chain_name': chain_name,
            'block_number': w3.eth.blockNumber
        }, index=[0])

def manage_data(web3_providers, start_date=None, end_date=None):
    data_frames = []
    for _, row in web3_providers.iterrows():
        print(f"Processing chain_id: {row['chain_id']} name: {row['chain_name']}")
        w3 = Web3(Web3.HTTPProvider(row['rpc_url']))
        w3.middleware_onion.inject(geth_poa_middleware, layer=0)
        data_frames.append(process_chain_data(w3, row['chain_name'], start_date, end_date))
        logger.info(f"chain_id: {row['chain_id']}, chain_name: {row['chain_name']} processed")
    return pd.concat(data_frames, axis=0)

def create_history(db_url, table_name, start_date, end_date=None):
    try:
        start_time = datetime.now()
        web3_providers = get_rpc_table()
        if end_date is None:
            end_date = datetime.now().strftime("%Y-%m-%d 00:00:00")
        block_table = manage_data(web3_providers, start_date, end_date)
        if table_exists(db_url, table_name):
            logger.info(f"Table already exists, changing new table name to {table_name}_new")
            table_name = f"{table_name}_new"
        save_table(db_url, table_name, block_table)
        logger.info(f"Create Block Table - Time elapsed: {datetime.now() - start_time}")
    except Exception as e:
        logger.error(f"Cannot create block table: {e}")

def update_history(db_url, table_name, end_date=None):
    try:
        start_time = datetime.now()
        block_table = get_table(db_url, table_name)
        last_date = datetime.fromtimestamp(block_table['timestamp'].max())
        if last_date.date() == datetime.now().date():
            logger.warning("Block table is up to date")
            return
        last_date += timedelta(days=1)
        if end_date is None:
            end_date = datetime.now().strftime("%Y-%m-%d 00:00:00")
        web3_providers = get_rpc_table()
        block_table = manage_data(web3_providers, last_date, end_date)
        update_table(db_url, table_name, block_table)
        logger.info(f"Update Block Table - Time elapsed: {datetime.now() - start_time}")
    except Exception as e:
        logger.error(f"Cannot update block table: {e}")

def create_current(db_url, table_name):
    try:
        start_time = datetime.now()
        web3_providers = get_rpc_table()
        block_table = manage_data(web3_providers)
        if table_exists(db_url, table_name):
            drop_table(db_url, table_name)
        save_table(db_url, table_name, block_table)
        logger.info(f"Create Current Block Table - Time elapsed: {datetime.now() - start_time}")
    except Exception as e:
        logger.error(f"Cannot create block table: {e}")

