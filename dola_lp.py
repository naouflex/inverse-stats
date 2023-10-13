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
import re

logger = logging.getLogger(__name__)
MAX_THREADS = 10

#logger writes to log.txt
logging.basicConfig(filename='log.txt',level=logging.ERROR)

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

def evaluate_operand(operand, w3, abi, prices, block_identifier, timestamp):
    try:
        
        if operand is None or pd.isnull(operand) or operand == '':
            return 0
        
        #first we need to determine the type of the formula : if starts with 0x it's a contract call
        if operand[0] == '0':
            contract, method = operand.split(':')
            method, argument = method.split('(')
            argument, indexes = argument.split(')')

            if '[' in indexes and '][' in indexes:
                index1, index2 = indexes.split('][')
                index1 = int(index1.replace('[',''))
                index2 = int(index2.replace(']',''))
            elif '[' in indexes and '][' not in indexes:
                index1 = indexes.replace('[','')
                index1 = int(index1.replace(']',''))
                index2 = None
            else:
                index1 = None
                index2 = None

            if len(argument) ==42:
                argument = w3.toChecksumAddress(argument)
            #print(f"Evaluating operand: {operand} with contract: {contract}, method: {method}, argument: {argument}, index1: {index1}, index2: {index2}")
            try:
                contract = w3.eth.contract(address=Web3.toChecksumAddress(contract), abi=abi)
                method = getattr(contract.functions, method)
                if argument is not None:
                    method = method(argument)
                result = method.call(block_identifier=int(block_identifier))
                
                if index1 is not None:
                    result = result[index1]
                    if index2 is not None:
                        result = result[index2]
                        #print(f"Call result : {result}")
                return result        
            except Exception as e:
                print(f"Error in evaluating method: {operand} : {e}")
                return 0
        elif operand[0] == '%':
            try:
                chain_slug, contract_address = operand[1:].split(':')
                price = prices[(prices['chain'] == chain_slug) & (prices['token_address'].str.lower() == contract_address.lower()) & (prices['timestamp'] <= int(timestamp))].iloc[-1]['price']
                
                if price is not None:
                    decimals = int(prices[(prices['chain'] == chain_slug) & (prices['token_address'].str.lower() == contract_address.lower())]['decimals'].iloc[0])
                    price = Decimal(price) / Decimal(10 ** int(decimals))
                return price or 0
            except Exception as e:
                print(f"Price cannot be found for {operand} : {e}")
                return 0

        
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


def process_row(row, prices, blocks,result):
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
            block_timestamp = blocks.iloc[i]['date']
            block_identifier = blocks.iloc[i][row['chain_name_y']]

            # if None or block_identifier < row['start_block'] or Nan
            if block_identifier is None or block_identifier < row['start_block'] or pd.isnull(block_identifier):
                continue

            formulae_asset = evaluate_formula(row['formula_asset'],w3,row['abi'],prices,block_identifier,block_timestamp)
            formulae_liability = evaluate_formula(row['formula_liability'],w3,row['abi'],prices,block_identifier,block_timestamp)

            result = result.append({'chain_id':row['chain_id'],
                                                'chain_name_x':row['chain_name_x'],
                                                'chain_name_y':row['chain_name_y'],
                                                'date':block_timestamp,
                                                'block_number':block_identifier,
                                                'protocol':row['protocol'],
                                                'account':row['account'],
                                                'name':row['Name'],
                                                'contract_address':row['contract_address'],
                                                'formula_asset':formulae_asset,
                                                'formula_liability':formulae_liability},ignore_index=True)

            print(formulae_asset)
            print(formulae_liability)

    except Exception as e:
        logger.error(f"Error in processing row : {e} : {traceback.format_exc()}")
        pass

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
        # subset blocks on date and row['chain_name_y']
        blocks_row = blocks[['date',row['chain_name_y']]]
        row_list.append((row,prices, blocks_row, result_state))

    threads = []
    
    # We want 10 threads at most concurrently running; when one stops, another one starts
    for row_data in row_list:
        t = threading.Thread(target=process_row, args=row_data)
        threads.append(t)
        t.start()
        while threading.active_count() > MAX_THREADS:
            pass
    
    for thread in threads:
        thread.join()
    
    result_state = pd.DataFrame(result_state)
    print(result_state)
    
    logger.info(f"Total execution time: {datetime.now() - start_time}")
    
except Exception as e:
    logger.error(traceback.format_exc())


