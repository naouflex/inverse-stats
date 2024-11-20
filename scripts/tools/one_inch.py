import json
import eth_abi
import requests
import time
import os
import pandas as pd
import traceback
import numpy as np
from brownie import ETH_ADDRESS, network, accounts, Contract, web3
from brownie.network.account import LocalAccount
from brownie.convert.datatypes import HexString
from dotenv import load_dotenv
from brownie.network.gas.strategies import LinearScalingStrategy
from brownie.exceptions import VirtualMachineError
from brownie.network.transaction import TransactionReceipt
from scripts.tools.common import df_to_result,fetch_abi
from types import SimpleNamespace
from scripts.tools.one_inch import get_1inch_quote, get_1inch_swap, init_network, print_trace

load_dotenv()

ETHERSCAN_API_KEY = '3H61BTZ5PN8RH9FVCVTM6WBVVZD92YHI79'
ONEINCH_API_KEY = os.getenv('ONEINCH_API_KEY')

ONE_INCH_ROUTER_ADDRESS = '{{ONE_INCH_AGGREGATOR_ADDRESS}}'
ROUTING_TOKENS =[{{ROUTING_TOKENS}}]
SELLER_ADDRESS = '{{SELLER_ADDRESS}}'
Y_TOKEN_ADDRESS = '{{Y_TOKEN_ADDRESS}}'
ST_Y_TOKEN_ADDRESS = '{{ST_Y_TOKEN_ADDRESS}}'
BUY_TOKEN_ADDRESS = '{{BUY_TOKEN_ADDRESS}}'
SWAP_AMOUNTS_USD = [{{SWAP_AMOUNTS_USD}}]

GAS_SRATEGY = ["70 gwei", "150 gwei", 1.1]

ORACLE_A_ADDRESS = '{{ORACLE_A_ADDRESS}}'
ORACLE_A_FUNCTION = '{{ORACLE_A_FUNCTION}}'
ORACLE_A_ARGUMENTS = [{{ORACLE_A_ARGUMENTS}}]
DECIMALS_A_OUTPUT = {{DECIMALS_A_OUTPUT}}

ORACLE_B_ADDRESS = '{{ORACLE_B_ADDRESS}}'
ORACLE_B_FUNCTION = '{{ORACLE_B_FUNCTION}}'
ORACLE_B_ARGUMENTS = [{{ORACLE_B_ARGUMENTS}}]
DECIMALS_B_OUTPUT = {{DECIMALS_B_OUTPUT}}


ONE_INCH_ROUTER_ABI = fetch_abi(ONE_INCH_ROUTER_ADDRESS, ETHERSCAN_API_KEY)
Y_TOKEN_ABI = fetch_abi(Y_TOKEN_ADDRESS, ETHERSCAN_API_KEY)
ST_Y_TOKEN_ABI = fetch_abi(ST_Y_TOKEN_ADDRESS, ETHERSCAN_API_KEY)
OUTPUT_ABI = fetch_abi(BUY_TOKEN_ADDRESS, ETHERSCAN_API_KEY)
ORACLE_A_ABI = fetch_abi(ORACLE_A_ADDRESS, ETHERSCAN_API_KEY)
ORACLE_B_ABI = fetch_abi(ORACLE_B_ADDRESS, ETHERSCAN_API_KEY)

try: 
    results = pd.DataFrame()

    for amount_usd in SWAP_AMOUNTS_USD:
        try:
            gas_strategy = init_network("mainnet-fork", GAS_SRATEGY)

            oracle_a_contract = Contract.from_abi("Oracle", address=ORACLE_A_ADDRESS, abi=ORACLE_A_ABI)
            oracle_b_contract = Contract.from_abi("Oracle", address=ORACLE_B_ADDRESS, abi=ORACLE_B_ABI)

            one_inch_router_contract = Contract.from_abi("OneInchRouter", address=ONE_INCH_ROUTER_ADDRESS, abi=ONE_INCH_ROUTER_ABI)
            y_token_contract = Contract.from_abi("Y_TOKEN", address=Y_TOKEN_ADDRESS, abi=Y_TOKEN_ABI)
            st_y_token_contract = Contract.from_abi("ST_Y_TOKEN", address=ST_Y_TOKEN_ADDRESS, abi=ST_Y_TOKEN_ABI)
            buy_token_contract = Contract.from_abi("BuyToken", address=BUY_TOKEN_ADDRESS, abi=OUTPUT_ABI)
            
            y_token_decimals = y_token_contract.decimals()
            st_y_token_decimals = st_y_token_contract.decimals()
            buy_token_contract_decimals = buy_token_contract.decimals()

            oracle_a_price = getattr(oracle_a_contract, ORACLE_A_FUNCTION)(*ORACLE_A_ARGUMENTS) / 10**DECIMALS_A_OUTPUT
            oracle_b_price = getattr(oracle_b_contract, ORACLE_B_FUNCTION)(*ORACLE_B_ARGUMENTS) / 10**DECIMALS_B_OUTPUT
            
            resp = requests.get(f"https://coins.llama.fi/prices/current/ethereum:{Y_TOKEN_ADDRESS}").json()
            
            if resp['coins'] == {}:
                print(f"Price is not available for token {Y_TOKEN_ADDRESS}. Please correct the address or wait a moment before retrying.")
                break

            current_sell_price = resp['coins'][f'ethereum:{Y_TOKEN_ADDRESS}']['price']
            amount_sold = amount_usd / current_sell_price
            #print(f"Amount sold: {amount_sold}")

            funder = accounts[0]
            funder.transfer(SELLER_ADDRESS, "1 ether", gas_price=gas_strategy)

            st_y_token_contract.withdraw(st_y_token_contract.balanceOf(SELLER_ADDRESS), {'from': accounts.at(SELLER_ADDRESS, force=True), 'gas_price': gas_strategy})
            y_token_contract.approve(ONE_INCH_ROUTER_ADDRESS, 2**256-1, {"from": accounts.at(SELLER_ADDRESS, force=True), 'gas_price': gas_strategy})

            seller_account = accounts.at(SELLER_ADDRESS, force=True)

            tx_events = get_1inch_swap(
                from_token=Y_TOKEN_ADDRESS,
                to_token=BUY_TOKEN_ADDRESS,
                from_amount=int(amount_sold * 10**y_token_decimals),
                slippage=int(50),
                allowPartialFill=True,
                min_expected_amount=0.1,
                seller_address=SELLER_ADDRESS,
                gas_price=gas_strategy,
                from_abi=Y_TOKEN_ABI,
                to_abi=OUTPUT_ABI
            )
            
            amount_bought = 0
            #print(f"Transaction events: {tx_events}")

            for event in tx_events:
                if event.name == "Transfer":
                    address_keys = ['to', '_to', 'dst', '_dst', 'receiver', '_receiver']
                    amount_keys = ['amount', '_amount', 'value', '_value','wad','_wad']
                    to_address = None
                    amount = None

                    for key in address_keys:
                        if key in event:
                            to_address = event[key]
                            break

                    for key in amount_keys:
                        if key in event:
                            amount = event[key]
                            break

                    if to_address and to_address.lower() == SELLER_ADDRESS.lower() and amount is not None:
                        amount_bought += amount

            amount_bought = amount_bought / 10**buy_token_contract_decimals

            resp = requests.get(f"https://coins.llama.fi/prices/current/ethereum:{BUY_TOKEN_ADDRESS}").json()
            
            if resp['coins'] == {}:
                print(f"Price is not available for token {BUY_TOKEN_ADDRESS}. Please correct the address or wait a moment before retrying.")
                break

            current_buy_price = resp['coins'][f'ethereum:{BUY_TOKEN_ADDRESS}']['price']

            oracle_a_price_new = getattr(oracle_a_contract, ORACLE_A_FUNCTION)(*ORACLE_A_ARGUMENTS) / 10**DECIMALS_A_OUTPUT
            oracle_b_price_new = getattr(oracle_b_contract, ORACLE_B_FUNCTION)(*ORACLE_B_ARGUMENTS) / 10**DECIMALS_B_OUTPUT

            abi = [{"constant": True, "inputs": [], "name": "symbol", "outputs": [{"name": "", "type": "string"}], "payable": False, "stateMutability": "view", "type": "function"}, {"constant": True, "inputs": [], "name": "decimals", "outputs": [{"name": "", "type": "uint8"}], "payable": False, "stateMutability": "view", "type": "function"}]
            
            
            transfer_events = [str(event.address) : {str(k): str(v) for k, v in event.items()} for event in tx_events['Transfer']]
            value = event['value'] / 10**contract.decimals()
            
            transfer_events = [
                {
                    'address': event.address,
                    'from': str(list(event.items())[0][1]),
                    'to': str(list(event.items())[1][1]),
                    'value': str(list(event.items())[-1][1])
                }
                for event in tx_events['Transfer']
            ]

            transfer_events_paragraph = f""

            for event in transfer_events:
                contract = Contract.from_abi("Contract", address=event['address'], abi=abi)
                symbol = contract.symbol()
                symbol =f'<a href="https://etherscan.io/token/{event["address"]}">{symbol}</a>'
                decimals = contract.decimals()
                # Ensure value is a float before formatting
                value = int(event['value']) / 10**decimals  # Convert event['value'] to int first if it's a string
                shortened_from_address = event["from"][:6] + "..." + event["from"][-4:]
                from_address_link = f'<a href="https://etherscan.io/address/{shortened_from_address}">{event["from"]}</a>'
                try:
                  from_name = Contract.from_abi("Contract", address=event['from'], abi=name_abi).name()
                except:
                    try: 
                        from_name = Contract.from_abi("Contract", address=event['from'], abi=abi).symbol()
                    except:
                        from_name = "Unknown"
                # Corrected quotation marks for to_address_link
                shortened_to_address = event["to"][:6] + "..." + event["to"][-4:]
                to_address_link = f'<a href="https://etherscan.io/address/{shortened_to_address}" target="_blank">{event["to"]}</a>'
                try:
                  to_name = Contract.from_abi("Contract", address=event['to'], abi=name_abi).name()
                except:
                    try: 
                        to_name = Contract.from_abi("Contract", address=event['to'], abi=abi).symbol()
                    except:
                        to_name = "Unknown"
                
                name_abi = [{"constant": True, "inputs": [], "name": "name", "outputs": [{"name": "", "type": "string"}], "payable": False, "stateMutability": "view", "type": "function"}]
                

                transfer_events_paragraph += f"{value:,.2f} {symbol} from {from_address_link} ({from_name}) to {to_address_link} ({to_name})"


            
                

            converted_tx_events = {
                'Transfer': transfer_events,
            }

            serialized_tx_events = json.dumps(converted_tx_events)
            results = results.append({
                'amount_bought': amount_bought,
                'amount_bought_usd': amount_bought * current_buy_price,
                'amount_sold_usd': amount_usd,
                'amount_sold': amount_sold,
                'oracle_a_price': oracle_a_price,
                'oracle_a_price_new': oracle_a_price_new,
                'a_diff': oracle_a_price_new - oracle_a_price,
                'a_diff_per': 100 * (oracle_a_price_new - oracle_a_price) / oracle_a_price,
                'oracle_b_price': oracle_b_price,
                'oracle_b_price_new': oracle_b_price_new,
                'b_diff': oracle_b_price_new - oracle_b_price,
                'b_diff_per': 100 * (oracle_b_price_new - oracle_b_price) / oracle_b_price,
                'events': serialized_tx_events,
                'transfer_events': transfer_events_paragraph
            }, ignore_index=True)

        except Exception as e:
            print(f"Error during swap for amount {amount_usd}: {e}")
            print(traceback.format_exc())
            continue

    result = df_to_result(results)

except Exception as e:
    print(f"Overall Error: {e}")
    print(traceback.format_exc())
    if hasattr(e, 'txid'):
        tx_receipt = TransactionReceipt(e.txid)
        print_trace(tx_receipt)
