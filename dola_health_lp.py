from datetime import datetime
from decimal import Decimal
import threading
import requests
import pandas as pd
import traceback
from web3 import Web3
from web3.middleware import geth_poa_middleware
from tools.database import save_table, get_table,update_table
import os
from dotenv import load_dotenv

load_dotenv()

def get_methodology_dola_health():
    try:
        methodology = requests.get("https://app.inverse.watch/api/queries/466/results.json?api_key=rr0yOmZp1wtw1owcQavb73QnAhHWJG356YX6Hj38").json()
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

        print(full_methodology)
        return full_methodology
    except Exception as e:
        print(f"Error in getting methodology : {e}")
        traceback.print_exc()
        return None

def get_rpc_table():
    try:
        web3_providers = requests.get("").json()
        web3_providers = pd.DataFrame(web3_providers['query_result']['data']['rows']) 
        return web3_providers
    except Exception as e:
        print(f"Cannot get rpc table: {e}")
        return
    
def create_db_table_current():
    return

def update_db_table_current():
    return

def create_db_table_history():
    return

def update_db_table_history():
    return

def process_row(row, blocks,result_state):
    try:
        contract_start_time = datetime.now()

        # get web3 and inject middle ware for PoA chains
        w3 = Web3(Web3.HTTPProvider(row['rpc_url']))
        w3.middleware_onion.inject(geth_poa_middleware, layer=0)
        contract = w3.eth.contract(address=Web3.toChecksumAddress(row['call_contract_address']), abi=(row['abi']))

        for block in blocks :
            if int(block) < int(row['start_block']) :
                continue
            try:
                function = getattr(contract.functions, row['function'])
                args = row['args']

                if row['account'] == 'fed':
                        value = function().call(block_identifier=block)
                        contract_address = "0x865377367054516e17014ccded1e7d814edc9ce4"
                        function= row['function']
                        args = row['call_contract_address']
                elif row['account'] == 'frontier':
                        value = function().call(block_identifier=block)
                        contract_address = row['collateral']
                        function= row['function']
                        args= row['call_contract_address']
                elif row['account'] == 'lp':
                    if row['function'] == 'getPoolTokens':                
                            value = function(args).call(block_identifier=block)
                            contract_address = value[0][value[0].index(j)]
                            function = row['function'], 
                            args = args[0:42], 
                            result= value[1][value[0].index(j)]
                    elif row['function'] == 'balanceOf':
                            if args[0:2] == '0x' and len(args) == 42:
                                args = Web3.toChecksumAddress(args)
                            value = function(args).call(block_identifier=int(block))
                            contract_address=row['call_contract_address'] 
                            function=row['function']

                result_state.append({'block_number': block,
                                    'block_timestamp': w3.eth.getBlock(block)['timestamp'],
                                    'contract_address': contract_address,
                                    'function': row['function'], 
                                    'args': args, 
                                    'result': value})
            
            except Exception as e:
                pass

        print(f"contract {row['name']} - {row['call_contract_address']} - {row['start_block']} to {row['last_block_number']} -  execution time {datetime.now() - contract_start_time}")

    except Exception as e:
        print(e)

try:
    start_time = datetime.now()
    
    # Fetch query results from API endpoints
    methodology = requests.get("https://app.inverse.watch/api/queries/466/results.json?api_key=rr0yOmZp1wtw1owcQavb73QnAhHWJG356YX6Hj38").json()
    web3_providers = requests.get(os.getenv("WEB3_PROVIDERS")).json()
    smart_contracts = requests.get("https://app.inverse.watch/api/queries/454/results.json?api_key=CNsPQor5gykZdi7jS746PngKK5M8KGeZsGvOZZPf").json()
    
    blocks = get_table(os.getenv('PROD_DB'),'blocks_daily')

    methodology = pd.DataFrame(methodology['query_result']['data']['rows'])
    web3_providers = pd.DataFrame(web3_providers['query_result']['data']['rows']) 
    smart_contracts = pd.DataFrame(smart_contracts['query_result']['data']['rows'])
    
    # Mount web3 clients and add last_block_number to df_data
    for i in range(len(web3_providers)):
        w3 = Web3(Web3.HTTPProvider(web3_providers['rpc_url'][i]))
        w3.middleware_onion.inject(geth_poa_middleware, layer=0)
        web3_providers.loc[i, 'last_block_number'] = w3.eth.blockNumber
    
    # Merge df_data and web3_providers on chain_id
    full_methodology = pd.merge(methodology, web3_providers, on='chain_id', how='left')
    full_methodology['call_contract_address'] = full_methodology['call_contract_address'].str.lower()
    smart_contracts['call_contract_address'] = smart_contracts['address'].str.lower()
    full_methodology = pd.merge(full_methodology, smart_contracts, on='call_contract_address', how='left')
    print(full_methodology)
    result_state = []
    row_list = []

    # TODO FIX logic
    for i in range(len(full_methodology)):
        row = full_methodology.iloc[i]
        # blocks are in the blocks dataframe in the column with a column name that correspond to the value in methodology['chain_name'] 
        # so for example we want to get the blocks for the polygon chain we get the blocks from blocks['polygon'] if chain_name is polygon in methodology  
        blocks_row = blocks[row['chain_name_y']]
        print(blocks_row)
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
    
    print(f"Total execution time: {datetime.now() - start_time}")
    
except Exception as e:
    print(traceback.format_exc())


