import threading
import requests
import pandas as pd
import traceback
import os
from datetime import datetime
from decimal import Decimal
from web3 import Web3
from web3.middleware import geth_poa_middleware
from tools.chainkit import get_call_result
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

def evaluate_formula(w3, abi, prices,string,block_identifier):
    #if string is none null empty or nan, return None
    if string is None or pd.isnull(string) or string == '':
        return None
    contract_address = string.split(':')[0]
    method_name = string.split(':')[1].split('(')[0]
    arguments = string.split(':')[1].split('(')[1][:-1].split(',')
    # if string contains index and sub index, then we need to get the index and sub index
    if '[' in string and ']' in string:
        index = string.split('[')[1].split(']')[0].split('][')[0]
        sub_index = string.split('[')[1].split(']')[0].split('][')[1]

    # if no arguments, then we call the method directly
    if len(arguments) == 1 and arguments[0] == '':
        return get_call_result(w3,contract_address,method_name,abi,[],block_identifier)
    # if len is 42 then we have a single argument and convert it to a checksum address
    elif len(arguments) == 1 and len(arguments[0]) == 42:
        return get_call_result(w3,contract_address,method_name,abi,[arguments[0]],block_identifier)
    # if len is 66 then we have a single argument and return usibng index and sub index
    elif len(arguments) == 1 and len(arguments[0]) == 66:
        try:
            return get_call_result(w3,contract_address,method_name,abi,[arguments[0]],block_identifier)[int(index)][int(sub_index)]
        except:
            print(f"Error in getting custom state for {string}")
            return get_call_result(w3,contract_address,method_name,abi,[arguments[0]],block_identifier)

def break_down_formula(string):
    # breakdown formula in parts sepereated by + - * / 
    formulae = []
    if string is None or pd.isnull(string) or string == '':
        return None
    if '+' in string or '-' in string or '*' in string or '/' in string:
        for i in string.split('+'):
            if '-' in i:
                for j in i.split('-'):
                    if '*' in j:
                        for k in j.split('*'):
                            if '/' in k:
                                for l in k.split('/'):
                                    formulae.append(l)
                            else:
                                formulae.append(k)
                    elif '/' in j:
                        for k in j.split('/'):
                            formulae.append(k)
                    else:
                        formulae.append(j)
            elif '*' in i:
                for j in i.split('*'):
                    if '/' in j:
                        for k in j.split('/'):
                            formulae.append(k)
                    else:
                        formulae.append(j)
            elif '/' in i:
                for j in i.split('/'):
                    formulae.append(j)
            else:
                formulae.append(i)
    else:
        formulae.append(string)
    return formulae

def process_row(row, prices, blocks_row,result_state):
    try:
        #blocks_row is a pd series
        contract_start_time = datetime.now()

        # get web3 and inject middle ware for PoA chains
        w3 = Web3(Web3.HTTPProvider(row['rpc_url']))
        w3.middleware_onion.inject(geth_poa_middleware, layer=0)
        contract = w3.eth.contract(address=Web3.toChecksumAddress(row['abi_address']), abi=(row['abi']))
        
        for block in blocks_row[blocks_row > row['start_block']]:
            #print(f"Processing block {block} for {row['chain_name_y']}")
            print(f"Formula Asset: {row['formula_asset']},value: {evaluate_formula(w3, row['abi'],prices, row['formula_asset'], block)}")
            print(f"Formula Asset: {row['formula_liability']},value: {evaluate_formula(w3, row['abi'],prices, row['formula_liability'], block)}")
            
            result_state.append({
                'chain_id': row['chain_id'],
                'chain_name': row['chain_name_y'],
                'contract_address': row['contract_address'],
                'protocol': row['protocol'],
                'account': row['account'],
                'name': row['name'],
                'start_block': row['start_block'],
                'value_liability': evaluate_formula(w3, row['abi'],prices, row['formula_liability'], block),
                'value_asset': evaluate_formula(w3, row['abi'],prices, row['formula_asset'], block),
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
    prices = get_table(os.getenv('PROD_DB'), 'defillama_prices')

    print(full_methodology)
    result_state = []
    row_list = []

    for i in range(len(full_methodology)):
        row = full_methodology.iloc[i]
        blocks_row = blocks[row['chain_name_y']]
        row_list.append((row,prices, blocks_row, result_state))

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
    print(result_state)
    
    logger.info(f"Total execution time: {datetime.now() - start_time}")
    
except Exception as e:
    logger.error(traceback.format_exc())


