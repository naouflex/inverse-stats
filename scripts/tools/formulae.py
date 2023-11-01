import re
import pandas as pd
import traceback
import json
import logging
import requests


from web3 import Web3
from web3.middleware import geth_poa_middleware
from decimal import Decimal
from scripts.tools.constants import WEB3_PROVIDERS_URL


logger = logging.getLogger(__name__)

def evaluate_operand(operand, abi,prices, block_identifier, timestamp,current):
    try:
        
        if operand is None or pd.isnull(operand) or operand == '':
            return 0
        
        #first we need to determine the type of the formula : if starts with 0x it's a contract call
        
        if operand[0] == '#':
            
            chain_id, contract = operand.split('$')
            chain_id = chain_id[1:]
            contract, method = contract.split(':')
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
            
            web3_providers = requests.get(WEB3_PROVIDERS_URL).json()
            web3_providers = pd.DataFrame(web3_providers['query_result']['data']['rows'])
            w3 = Web3(Web3.HTTPProvider(web3_providers[web3_providers['chain_id'] == int(chain_id)]['rpc_url'].iloc[0]))
            w3.middleware_onion.inject(geth_poa_middleware, layer=0)

            if len(argument) ==42:
                argument = w3.toChecksumAddress(argument)

            try:
                contract = w3.eth.contract(address=Web3.toChecksumAddress(contract), abi=json.loads(abi))
                method = getattr(contract.functions, method)
                if argument is not None and argument != '':
                    method = method(argument)
                else:
                    method = method()
                result = method.call(block_identifier=int(block_identifier))
                
                if index1 is not None:
                    result = result[index1]
                    if index2 is not None:
                        result = result[index2]
                        #logger.info(f"Call result : {result}")
                return result        
            except Exception as e:
                logger.info(f"Error in evaluating method: {operand} : {e}")
                return 0
            
        elif operand[0] == '%':
            try:
                chain_slug, contract_address = operand[1:].split(':')

                if not current :
                    filtered_prices = prices[
                        (prices['chain'] == chain_slug) &
                        (prices['token_address'].str.lower() == contract_address.lower()) &
                        (prices['timestamp'] <= int(timestamp)) &
                        (prices['timestamp'] >= int(timestamp) - 86400)*7
                    ]
                else:
                    filtered_prices = prices[
                        (prices['chain'] == chain_slug) &
                        (prices['token_address'].str.lower() == contract_address.lower()) 
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
                logger.info(f"Price cannot be found for {operand} at timestamp {timestamp}")
                return 0
            
        else:
            return float(operand)

    except Exception as e:
        logger.error(traceback.format_exc())
        return 0

# Apply operator with None value and type handling
def apply_operator(operator, operand1, operand2):
    #logger.info(f"Applying operator: {operator}, operand1: {operand1} (type: {type(operand1)}), operand2: {operand2} (type: {type(operand2)})")
    
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
            logger.error(f"Division by zero : {operand1} / {operand2}")
            return 0
    else:
        return 0

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
def evaluate_postfix(postfix, abi, prices, block_identifier, block_timestamp,current):
    stack = []
    for element in postfix:
        if element in ['+', '-', '*', '/']:
            operand2 = stack.pop()
            operand1 = stack.pop()
            stack.append(apply_operator(element, operand1, operand2))
        else:
            stack.append(evaluate_operand(element,abi, prices, block_identifier, block_timestamp,current))
    return stack[0]

# Evaluate formula
def evaluate_formula(string,abi,prices,block_identifier,block_timestamp,current):
    if string is None or pd.isnull(string) or string == '':
        return None
    
    # Tokenize the formula into its basic elements
    parts = re.split(r'([\+\-\*/])', string)
    parts = [part.strip() for part in parts if part.strip() != '']
    
    # Convert infix to postfix using Shunting Yard Algorithm
    postfix = shunting_yard_infix_to_postfix(parts)
    
    # Evaluate the postfix expression
    return evaluate_postfix(postfix, abi,prices, block_identifier, block_timestamp,current)


def build_methodology_table(methodology_url,web3_providers_url):
    try:
        methodology = requests.get(methodology_url).json()
        web3_providers = requests.get(web3_providers_url).json()

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