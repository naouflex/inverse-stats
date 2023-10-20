import threading
import requests
import pandas as pd
import traceback
import os
from datetime import datetime
from decimal import Decimal
from web3 import Web3
from web3.middleware import geth_poa_middleware
from scripts.tools.database import drop_table, save_table, get_table, table_exists,update_table
from dotenv import load_dotenv
import logging
import re
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)
MAX_THREADS = 12

lock = threading.Lock()
load_dotenv()

def build_methodology_table():
    try:
        methodology = requests.get("https://app.inverse.watch/api/queries/499/results.json?api_key=hPTTHXRhBI36YiK4UYrYzFA481GheipJLJ1ubIOA").json()
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
        print(f"Error in getting methodology : {e}")
        traceback.print_exc()
        return None

def evaluate_operand(operand, w3, abi, prices, block_identifier, timestamp):
    try:
        
        if operand is None or pd.isnull(operand) or operand == '':
            return 0
        
        #first we need to determine the type of the formula : if starts with 0x it's a contract call
        if operand[0] == '0':
            contract, method = operand.split(':')
            method, argument = method.split('(')
            argument, indexes = argument.split(')')

            if len(argument) ==42:
                argument = w3.toChecksumAddress(argument)

            try:
                contract = w3.eth.contract(address=Web3.toChecksumAddress(contract), abi=abi)
                method = getattr(contract.functions, method)
                if argument is not None and argument != '':
                    method = method(argument)
                else:
                    method = method()
                result = method.call(block_identifier=int(block_identifier))
                
                return result        
            except Exception as e:
                print(f"Error in evaluating method: {operand} : {e}")
                return 0
            
        elif operand[0] == '%':
            try:
                chain_slug, contract_address = operand[1:].split(':')
                
                filtered_prices = prices[
                    (prices['chain'] == chain_slug) &
                    (prices['token_address'].str.lower() == contract_address.lower()) &
                    (prices['timestamp'] <= int(timestamp))
                ]

                # Check if the DataFrame is empty
                if filtered_prices.empty:
                    raise ValueError(f"Price cannot be found for %s:%s" % (chain_slug, contract_address))
                else:
                    # Sort by timestamp to make sure the latest price is at the end
                    sorted_prices = filtered_prices.sort_values(by='timestamp')
                    price = sorted_prices.iloc[-1]['price']

                if price is not None:
                    decimals = int(prices[(prices['chain'] == chain_slug) & (prices['token_address'].str.lower() == contract_address.lower())]['decimals'].iloc[0])
                    price = Decimal(price) / Decimal(10 ** int(decimals))
                return price or 0
            except Exception as e:
                print(f"Price cannot be found for {operand} at timestamp {timestamp}")
                return 0
        else:
            return float(operand)

    except Exception as e:
        print(traceback.format_exc())
        return 0

# Apply operator with None value and type handling
def apply_operator(operator, operand1, operand2):
    #print(f"Applying operator: {operator}, operand1: {operand1} (type: {type(operand1)}), operand2: {operand2} (type: {type(operand2)})")
    
    if operand1 is None or operand2 is None:
        return None
    
    # Explicitly cast to Decimal if the operands are not None
    from decimal import Decimal
    operand1 = Decimal(operand1) if operand1 is not None else None
    operand2 = Decimal(operand2) if operand2 is not None else None
    
    if operator == '+':
        return operand1 + operand2
    elif operator == '-':
        return operand1 - operand2
    elif operator == '*':
        return operand1 * operand2
    elif operator == '/':
        if operand2 != 0:
            return operand1 / operand2
        else:
            raise ValueError("Division by zero")
    else:
        return None

# Get operator precedence
def precedence(operator):
    return {'+': 1, '-': 1, '*': 2, '/': 2}.get(operator, 0)

# Convert infix to postfix using Shunting Yard Algorithm
def shunting_yard_infix_to_postfix(parts):
    output = []
    operators = []
    for part in parts:
        if part in ['+', '-', '*', '/']:
            while operators and precedence(operators[-1]) >= precedence(part):
                output.append(operators.pop())
            operators.append(part)
        else:
            output.append(part)
    while operators:
        output.append(operators.pop())

    return output

# Evaluate postfix expression
def evaluate_postfix(postfix, w3,abi, prices, block_identifier, block_timestamp):
    stack = []
    for element in postfix:
        if element in ['+', '-', '*', '/']:
            operand2 = stack.pop()
            operand1 = stack.pop()
            stack.append(apply_operator(element, operand1, operand2))
        else:
            stack.append(evaluate_operand(element, w3,abi, prices, block_identifier, block_timestamp))
    return stack[0]

# Evaluate formula
def evaluate_formula(string,w3,abi,prices,block_identifier,block_timestamp):
    if string is None or pd.isnull(string) or string == '':
        return None
    
    # Tokenize the formula into its basic elements
    parts = re.split(r'([\+\-\*/])', string)
    parts = [part.strip() for part in parts if part.strip() != '']
    
    # Convert infix to postfix using Shunting Yard Algorithm
    postfix = shunting_yard_infix_to_postfix(parts)
    
    # Evaluate the postfix expression
    return evaluate_postfix(postfix, w3, abi,prices, block_identifier, block_timestamp)

def validate_keys(data):
    valid_keys = {k: v for k, v in {
        'timestamp': 'Int64',
        'block_number': 'Int64',
        'account_id': 'string',
        'chain_id': 'Int64',
        'chain_name_x': 'string',
        'contract_address': 'string',
        'protocol': 'string',
        'account_type': 'string',
        'contract_name': 'string',
        'collateral_addess': 'string',
        'fed_address': 'string',
        'formula_asset': 'float64',
        'formula_liability': 'float64'
    }.items() if k in data.columns}
    
    for col, new_type in valid_keys.items():
        try:
            data[col] = data[col].astype(new_type)
        except TypeError:
            print(f"Failed to cast column {col} to {new_type}")
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
            today_timestamp_at_midnight = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).timestamp()

            # if None or block_identifier < row['start_block'] or Nan
            if block_identifier is None or block_identifier < row['start_block'] or pd.isnull(block_identifier):
                continue
            try :
                formulae_asset = evaluate_formula(row['formula_asset'],w3,row['abi'],prices,block_identifier,block_timestamp)
            except Exception as e:
                formulae_asset = 'Error'
                print(f"Error in evaluating formulae_asset : {e} : {traceback.format_exc()}")
            try:
                formulae_liability = evaluate_formula(row['formula_liability'],w3,row['abi'],prices,block_identifier,block_timestamp)
            except Exception as e:
                formulae_liability = 'Error'
                print(f"Error in evaluating formulae_liability : {e} : {traceback.format_exc()}")
            
            temp_data = {
                    'timestamp':block_timestamp,
                    'block_number':block_identifier,
                    'account_id':row['account_id'],
                    'chain_id':row['chain_id_x'],
                    'chain_name_x':row['chain_name_x'],
                    'contract_address':row['contract_address'],
                    'protocol':row['protocol'],
                    'account_type':row['account_type'],
                    'type':row['type'],
                    'contract_name':row['contract_name'],
                    'collateral_address':row['collateral_address'],
                    'fed_adress':row['fed_address'],
                    'formula_asset':formulae_asset,
                    'formula_liability':formulae_liability
                }

            with lock:
                data.append(temp_data)
                
        print(f"Processed row {row['contract_name']} in {datetime.now() - contract_start_time}")

    except Exception as e:
        print(f"Error in processing row : {e} : {traceback.format_exc()} results : {formulae_asset} : {formulae_liability}")
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

        validate_keys(data)

        if table_exists(db_url, table_name):
            drop_table(db_url, table_name)
        save_table(db_url,table_name,data)

        print(f"Total execution time: {datetime.now() - start_time}")
        
    except Exception as e:
        print(traceback.format_exc())
        print(f"Total execution time: {datetime.now() - start_time}")

def update_history(db_url,table_name):
    try:
        start_time = datetime.now()
        full_methodology = build_methodology_table()

        current_data = get_table(db_url,table_name)
        #print(f"Current data : {(current_data)}")

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
                        print('It seems blocks daily table is out of sync, please update it before proceeding.')
                        return
                    elif row_latest_timestamp == today_timestamp:
                        print(f"Skipping row {row['contract_name']} because it was already updated.")
                        continue
            
                print(f"Updating Row {row['contract_name']} latest timestamp: {row_latest_timestamp}, today timestamp: {today_timestamp}, blocks to scan: {blocks_to_read}")

            else:
                print(f"Row {row['contract_name']} was not found in the current data, updating from scratch.")


            row_list.append((row,prices, blocks_to_read, data))

        with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            for row_data in row_list:
                executor.submit(process_row, *row_data)

        data = pd.DataFrame(data)

        validate_keys(data)

        update_table(db_url,table_name,data)

        print(f"Total execution time: {datetime.now() - start_time}")
        
    except Exception as e:
        print(traceback.format_exc())
        print(row)
        print(f"Total execution time: {datetime.now() - start_time}")

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

        for col in data.columns:
            if data[col].isnull().any():
                print(f"Column {col} has NaN values.")
            if data[col].dtype == 'float64':
                non_integers = data[col][~data[col].apply(lambda x: x.is_integer())]
                if not non_integers.empty:
                    print(f"Column {col} has non-integer float numbers: {non_integers}")

        data['timestamp'] = data['timestamp'].astype('Int64')

        validate_keys(data)

        if table_exists(db_url, table_name):
            drop_table(db_url, table_name)
        save_table(db_url,table_name,data)

        print(f"Total execution time: {datetime.now() - start_time}")
        
    except Exception as e:
        print(traceback.format_exc())
        print(f"Total execution time: {datetime.now() - start_time}")
    return