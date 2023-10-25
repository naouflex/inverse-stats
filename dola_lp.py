import threading
import requests
import pandas as pd
import traceback
import logging
import os

from web3 import Web3
from datetime import datetime
from dotenv import load_dotenv
from web3.middleware import geth_poa_middleware
from concurrent.futures import ThreadPoolExecutor

from scripts.tools.database import drop_table, save_table, get_table, table_exists,update_table
from scripts.tools.formulae import evaluate_formula

logger = logging.getLogger(__name__)
MAX_THREADS = 10

lock = threading.Lock()
load_dotenv()

def build_methodology_table():
    try:
        methodology = requests.get("https://app.inverse.watch/api/queries/479/results.json?api_key=zCljA8HpUclyQQ4xHH3mpIaCBhjtjf2ljTd77Y9V").json()
        web3_providers = requests.get(os.getenv("WEB3_PROVIDERS")).json()

        methodology = pd.DataFrame(methodology['query_result']['data']['rows'])
        web3_providers = pd.DataFrame(web3_providers['query_result']['data']['rows']) 

        for i in range(len(web3_providers)):
            w3 = Web3(Web3.HTTPProvider(web3_providers['rpc_url'][i]))
            w3.middleware_onion.inject(geth_poa_middleware, layer=0)
            web3_providers.loc[i, 'last_block_number'] = w3.eth.blockNumber

        full_methodology = pd.merge(methodology, web3_providers, on='chain_id', how='left')
        
        return full_methodology
    
    except Exception as e:
        logger.error(f"Error in getting methodology : {e}")
        return None

def validate_keys(data):
        valid_keys = {k: v for k, v in {
            'timestamp': 'Int64', 
            'block_number': 'Int64',
            'account_id': 'Int64',
            'chain_id': 'Int64',
            'chain_name_x': 'string',
            'contract_address': 'string',
            'protocol': 'string',
            'contract_name': 'string',
            'account_type': 'string',
            'collateral_type': 'string',
            'lp_type': 'string',
            'fed_address': 'string',
            'formula_asset': 'float64',
            'formula_liability': 'float64'
        }.items() if k in data.columns}

        for col, new_type in valid_keys.items():
            try:
                data[col] = data[col].astype(new_type)
            except TypeError:
                logger.error(f"Failed to cast column {col} to {new_type}")
                pass
        return

def process_row(row, prices, blocks,data):
    try:
        #blocks_row is a pd series
        contract_start_time = datetime.now()

        # get web3 and inject middle ware for PoA chains
        w3 = Web3(Web3.HTTPProvider(row['rpc_url']))
        w3.middleware_onion.inject(geth_poa_middleware, layer=0)
        
        #filter out blocks lower than row['start_block'] and NaN or None values
        blocks = blocks[blocks[row['chain_name_y']] >= row['start_block']]
        blocks = blocks[blocks[row['chain_name_y']].notnull()]

        for i in range(len(blocks)):
            block_timestamp = blocks.iloc[i]['timestamp']
            block_identifier = blocks.iloc[i][row['chain_name_y']]

            if block_identifier is None or block_identifier < row['start_block'] or pd.isnull(block_identifier):
                continue

            try :
                formulae_asset = evaluate_formula(row['formula_asset'],w3,row['abi'],prices,block_identifier,block_timestamp)
            except Exception as e:
                formulae_asset = 0
                logger.error(f"Error in evaluating formula_asset : {e} : {traceback.format_exc()}")

            try:
                formulae_liability = evaluate_formula(row['formula_liability'],w3,row['abi'],prices,block_identifier,block_timestamp)
            except Exception as e:
                formulae_liability = 0
                logger.error(f"Error in evaluating formula_liability : {e} : {traceback.format_exc()}")
            
            temp_data = {
                    'timestamp':block_timestamp,
                    'block_number':block_identifier,
                    'account_id':row['account_id'],
                    'chain_id':row['chain_id'],
                    'chain_name_x':row['chain_name_x'],
                    'contract_address':row['contract_address'],
                    'protocol':row['protocol'],
                    'contract_name':row['contract_name'],
                    'account_type':row['account_type'],
                    'collateral_type':row['collateral_type'],
                    'lp_type':row['lp_type'],
                    'fed_address':row['fed_address'],
                    'formula_asset':formulae_asset,
                    'formula_liability':formulae_liability
                }

            with lock:
                data.append(temp_data)
                
        logger.info(f"Processed row {row['contract_name']} in {datetime.now() - contract_start_time}")

    except Exception as e:
        logger.error(f"Error in processing row : {e} : {traceback.format_exc()} results : {formulae_asset} : {formulae_liability}")
        pass

def create_history(db_url,table_name):
    try:
        start_time = datetime.now()
        full_methodology = build_methodology_table()

        blocks = get_table(os.getenv('PROD_DB'), 'blocks_daily')
        prices = get_table(os.getenv('PROD_DB'), 'defillama_prices')
        
        data = []
        row_list = []

        for i in range(len(full_methodology)):
            row = full_methodology.iloc[i]
            # subset blocks on date and row['chain_name_y']
            blocks_row = blocks[['timestamp',row['chain_name_y']]]
            row_list.append((row,prices, blocks_row, data))

        with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            for row_data in row_list:
                executor.submit(process_row, *row_data)

        data = pd.DataFrame(data)

        # Filter out any keys not in DataFrame columns
        validate_keys(data)

        save_table(db_url,table_name,data)

        logger.info(f"Total execution time: {datetime.now() - start_time}")
        
    except Exception as e:
        logger.error(traceback.format_exc())
        logger.info(f"Total execution time: {datetime.now() - start_time}")

def update_history(db_url,table_name):
    try:
        start_time = datetime.now()
        full_methodology = build_methodology_table()

        current_data = get_table(db_url,table_name)
        # get latest timestamp and block_number for each contract in the db
        latest_blocks = current_data.groupby(['contract_address']).agg({'timestamp': 'max', 'block_number': 'max'}).reset_index()

        blocks = get_table(os.getenv('PROD_DB'), 'blocks_daily')
        prices = get_table(os.getenv('PROD_DB'), 'defillama_prices')
        
        data = []
        row_list = []

        for i in range(len(full_methodology)):
            row = full_methodology.iloc[i]
            blocks_to_read = blocks[['timestamp', row['chain_name_y']]]

            if row['contract_address'] in current_data['contract_address'].values:
                row_latest_block = latest_blocks[latest_blocks['contract_address'] == row['contract_address']]['block_number'].iloc[0]
                row_latest_timestamp = latest_blocks[latest_blocks['contract_address'] == row['contract_address']]['timestamp'].iloc[0]
                today_timestamp = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
                blocks_to_read = blocks_to_read[blocks_to_read[row['chain_name_y']] > row_latest_block]

                if blocks_to_read.empty:
                    if row_latest_timestamp < today_timestamp:
                        logger.error('It seems blocks daily table is out of sync, please update it before proceeding.')
                        return
                    elif row_latest_timestamp == today_timestamp:
                        logger.warning(f"Skipping row {row['contract_name']} because it was already updated.")
                        continue
                logger.warning(f"Updating Row {row['contract_name']} latest timestamp: {row_latest_timestamp}, today timestamp: {today_timestamp}, blocks to scan: {blocks_to_read}")
            else:
                logger.warning(f"Row {row['contract_name']} was not found in the current data, updating from scratch.")

            row_list.append((row,prices, blocks_to_read, data))

        with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            for row_data in row_list:
                executor.submit(process_row, *row_data)

        data = pd.DataFrame(data)

        validate_keys(data)
        update_table(db_url,table_name,data)

        logger.info(f"Total execution time: {datetime.now() - start_time}")
        
    except Exception as e:
        logger.error(traceback.format_exc())
        logger.info(row)

def create_current(db_url,table_name):
    # save as above but only for the last_block_number
    try:
        start_time = datetime.now()
        full_methodology = build_methodology_table()

        blocks = get_table(os.getenv('PROD_DB'), 'blocks_current')
        prices = get_table(os.getenv('PROD_DB'), 'defillama_prices_current')
        
        data = []
        row_list = []

        for i in range(len(full_methodology)):
            row = full_methodology.iloc[i]
            
            blocks_row = blocks[blocks['chain_name'] == row['chain_name_y']]
            #reshape with timestamp: date and chain_name_y: block_number
            blocks_row = blocks_row.rename(columns={'block_number': row['chain_name_y']})
            blocks_row = blocks_row.drop_duplicates(subset=['timestamp', row['chain_name_x']], keep='last')
            
            # Update blocks_row so we can access bloxks_row['timestamp'] and blocks_row[row['chain_name_y']]
            blocks_row = blocks_row.set_index('timestamp')
            blocks_row = blocks_row[[row['chain_name_y']]]
            
            blocks_row = blocks_row.reset_index()
            blocks_row['timestamp'] = start_time.timestamp()
            # make sure the timestamp is an int
            blocks_row['timestamp'] = blocks_row['timestamp'].astype(int)

            row_list.append((row,prices, blocks_row, data))

        with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            for row_data in row_list:
                executor.submit(process_row, *row_data)

        data = pd.DataFrame(data)

        data['timestamp'] = data['timestamp'].astype('Int64')

        validate_keys(data)

        if table_exists(db_url, table_name):
            drop_table(db_url, table_name)
        save_table(db_url,table_name,data)

        logger.info(f"Total execution time: {datetime.now() - start_time}")
        
    except Exception as e:
        logger.error(traceback.format_exc())