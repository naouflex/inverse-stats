import threading
import requests
import pandas as pd
import traceback
import os
from datetime import datetime
from decimal import Decimal
from web3 import Web3
from web3.middleware import geth_poa_middleware
from tools.database import save_table, get_table,update_table
from dotenv import load_dotenv
import logging

logger = logging.getLogger(__name__)

load_dotenv()

def build_methodology_table():
    try:
        methodology = requests.get("https://app.inverse.watch/api/queries/479/results.json?api_key=zCljA8HpUclyQQ4xHH3mpIaCBhjtjf2ljTd77Y9V").json()
        web3_providers = requests.get(os.getenv("WEB3_PROVIDERS")).json()
        smart_contracts = requests.get("https://app.inverse.watch/api/queries/454/results.json?api_key=CNsPQor5gykZdi7jS746PngKK5M8KGeZsGvOZZPf").json()

        methodology = pd.DataFrame(methodology['query_result']['data']['rows'])
        web3_providers = pd.DataFrame(web3_providers['query_result']['data']['rows']) 
        smart_contracts = pd.DataFrame(smart_contracts['query_result']['data']['rows'])

        for i in range(len(web3_providers)):
            w3 = Web3(Web3.HTTPProvider(web3_providers['rpc_url'][i]))
            w3.middleware_onion.inject(geth_poa_middleware, layer=0)
            web3_providers.loc[i, 'last_block_number'] = w3.eth.blockNumber

        full_methodology = pd.merge(methodology, web3_providers, on='chain_id', how='left')
        full_methodology['abi_address'] = full_methodology['abi_address'].str.lower()
        smart_contracts['abi_address'] = smart_contracts['address'].str.lower()
        full_methodology = pd.merge(full_methodology, smart_contracts, on='abi_address', how='left')

        return full_methodology
    
    except Exception as e:
        logger.error(f"Error in getting methodology : {e}")
        traceback.print_exc()
        return None

def get_rpc_table():
    try:
        web3_providers = requests.get("").json()
        web3_providers = pd.DataFrame(web3_providers['query_result']['data']['rows']) 
        return web3_providers
    except Exception as e:
        logger.error(f"Cannot get rpc table: {e}")
        return

def process_row(row, blocks_row,result_state):
    try:
        #blocks_row is a pd series
        contract_start_time = datetime.now()

        # get web3 and inject middle ware for PoA chains
        w3 = Web3(Web3.HTTPProvider(row['rpc_url']))
        w3.middleware_onion.inject(geth_poa_middleware, layer=0)
        contract = w3.eth.contract(address=Web3.toChecksumAddress(row['abi_address']), abi=(row['abi']))
        
        for block in blocks_row[blocks_row > row['start_block']]:
            print(f"Processing block {block} for {row['chain_name_y']}")
            formula_asset = row['formula_asset']
            formula_liability = row['formula_liability']
            
    except Exception as e:
        print(traceback.format_exc())

try:
    start_time = datetime.now()
    full_methodology = build_methodology_table()
    blocks = get_table(os.getenv('PROD_DB'), 'blocks_daily')

    print(full_methodology)
    result_state = []
    row_list = []

    for i in range(len(full_methodology)):
        row = full_methodology.iloc[i]
        blocks_row = blocks[row['chain_name_y']]
        blocks_row.name = 'block_number'
        blocks_row = blocks_row.dropna()
        blocks_row = blocks_row[blocks_row != '']
        blocks_row = blocks_row.astype(int)

        row_list.append((row, blocks_row, result_state))

    threads = []
    
    # We want 10 threads at most concurrently running; when one stops, another one starts
    for row_data in row_list:
        t = threading.Thread(target=process_row, args=row_data)
        threads.append(t)
        t.start()
        while threading.active_count() > 30:
            pass
    
    for thread in threads:
        thread.join()
    
    result_state = pd.DataFrame(result_state)
    
    logger.info(f"Total execution time: {datetime.now() - start_time}")
    
except Exception as e:
    logger.error(traceback.format_exc())


