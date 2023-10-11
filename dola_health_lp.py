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
from tools.chainkit import get_custom_state
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

def process_row(row, blocks_row,result_state):
    try:
        #blocks_row is a pd series
        contract_start_time = datetime.now()

        # get web3 and inject middle ware for PoA chains
        w3 = Web3(Web3.HTTPProvider(row['rpc_url']))
        w3.middleware_onion.inject(geth_poa_middleware, layer=0)
        contract = w3.eth.contract(address=Web3.toChecksumAddress(row['abi_address']), abi=(row['abi']))
        
        for block in blocks_row[blocks_row > row['start_block']]:
            #print(f"Processing block {block} for {row['chain_name_y']}")
            print(f"Formula Asset: {row['formula_asset']},value: {get_custom_state(w3, row['abi'], row['formula_asset'], block)}")
            print(f"Formula Asset: {row['formula_liability']},value: {get_custom_state(w3, row['abi'], row['formula_liability'], block)}")
            
            # if there is a formula for liability, first we check if there is a + in the formula
            # if there is a + in the formula, then we need to split the formula into two parts and get the value before applying the formula
            # if there is no + in the formula, then we just apply the formula
            
            #then we inswert the result into the result state
            # id	chain_id	chain_name	type	abi_address	contract_address	protocol	account	Name	start_block	function	value_liability	value_asset
            result_state.append({
                'chain_id': row['chain_id'],
                'chain_name': row['chain_name_y'],
                'contract_address': row['contract_address'],
                'protocol': row['protocol'],
                'account': row['account'],
                'name': row['name'],
                'start_block': row['start_block'],
                'value_liability': get_custom_state(w3, row['abi'], row['formula_liability'], block),
                'value_asset': get_custom_state(w3, row['abi'], row['formula_asset'], block),
                'block_number': block,
                'block_timestamp': w3.eth.getBlock(block).timestamp,
                'last_updated': datetime.now(),
            })


            
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
        row_list.append((row, blocks_row, result_state))

    threads = []
    
    # We want 10 threads at most concurrently running; when one stops, another one starts
    for row_data in row_list:
        t = threading.Thread(target=process_row, args=row_data)
        threads.append(t)
        t.start()
        while threading.active_count() > 1:
            pass
    
    for thread in threads:
        thread.join()
    
    result_state = pd.DataFrame(result_state)
    
    logger.info(f"Total execution time: {datetime.now() - start_time}")
    
except Exception as e:
    logger.error(traceback.format_exc())


