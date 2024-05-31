
import json
from brownie import ETH_ADDRESS, network, accounts, Contract, web3
from brownie.network.account import LocalAccount
from brownie.convert.datatypes import HexString
from dotenv import load_dotenv
from brownie.network.gas.strategies import LinearScalingStrategy
from brownie.exceptions import VirtualMachineError
from brownie.network.transaction import TransactionReceipt
import eth_abi
import requests
import time
import os
import pandas as pd
import traceback
from types import SimpleNamespace
import logging

logger = logging.getLogger(__name__)
load_dotenv()

ETHERSCAN_API_KEY = os.getenv('ETHERSCAN_API_KEY')

ONEINCH_API_KEY = os.getenv('ONEINCH_API_KEY')
ONE_INCH_ROUTER_ADDRESS = '0x1111111254eeb25477b68fb85ed929f73a960582'
ONE_INCH_ROUTER_ABI = json.loads(requests.get(f"https://api.etherscan.io/api?module=contract&action=getabi&address={ONE_INCH_ROUTER_ADDRESS}&apikey={ETHERSCAN_API_KEY}").json()['result'])

def init_network(name, GAS_STRATEGY):
        try:
          if network.is_connected():
              network.disconnect()
          network.connect(name)
          if not network.is_connected():
              raise Exception("Failed to connect to the network")
          
          gas_strategy = LinearScalingStrategy(GAS_STRATEGY[0], GAS_STRATEGY[1], GAS_STRATEGY[2])
          return gas_strategy
        except Exception as e:
          logger.error(f"An error occurred: {e}")
          logger.error(traceback.format_exc())
          return 0

def get_1inch_quote(from_token, to_token, from_amount, retry_on_ratelimit=True):
  try:
    if len(ONEINCH_API_KEY) <= 0:
      raise Exception("Missing API key")
    res = requests.get('https://api.1inch.dev/swap/v5.2/1/quote', params={
      'src': from_token,
      'dst': to_token,
      'amount': str(from_amount)
    }, headers={
      'accept': 'application/json',
      'Authorization': 'Bearer {}'.format(ONEINCH_API_KEY)
    })

    if retry_on_ratelimit and res.status_code == 429:
      time.sleep(2) # Wait for 2s and then try again
      return get_1inch_quote(from_token, to_token, from_amount, False)
    elif res.status_code != 200:
      print(res)
      raise Exception("Error accessing 1inch api")

    result = json.loads(res.text)

    return int(result['toAmount'])
  except Exception as e:
    logger.error(f"An error occurred: {e}")
    logger.error(traceback.format_exc())
    return 0

def get_1inch_swap_data(from_token, to_token, swap_amount, slippage, from_address, to_address, retry_on_ratelimit=True):
  try:
    if len(ONEINCH_API_KEY) <= 0:
      raise Exception("Missing API key")

    res = requests.get('https://api.1inch.dev/swap/v5.2/1/swap', params={
      'src': from_token,
      'fromAddress': from_address,
      'receiver': to_address,
      'dst': to_token,
      'amount': str(swap_amount),
      'allowPartialFill': True,
      'disableEstimate': 'true',
      'slippage': slippage
    }, headers={
      'accept': 'application/json',
      'Authorization': 'Bearer {}'.format(ONEINCH_API_KEY)
    })

    if retry_on_ratelimit and res.status_code == 429:
      time.sleep(2) # Wait for 2s and then try again
      return get_1inch_swap_data(from_token, to_token, swap_amount, slippage, from_address, to_address, False)
    elif res.status_code != 200:
      print(res.text)
      raise Exception("Error accessing 1inch api")

    result = json.loads(res.text)

    return SimpleNamespace(receiver = result['tx']['to'], input = result['tx']['data'])
  except Exception as e:
    logger.error(f"An error occurred: {e}")
    logger.error(traceback.format_exc())
    return 0

def get_1inch_swap(from_token,to_token,from_amount,slippage, allowPartialFill,min_expected_amount,seller_address,gas_price,from_abi,to_abi):
    try:
      router_1inch = Contract.from_abi("OneInchRouter",address=ONE_INCH_ROUTER_ADDRESS,abi=ONE_INCH_ROUTER_ABI)
      from_token_contract = Contract.from_abi("FromToken", address=from_token, abi=from_abi)
      to_token_contract = Contract.from_abi("ToToken", address=to_token, abi=to_abi)
      SWAP_SELECTOR = "0x12aa3caf" #swap(address,(address,address,address,address,uint256,uint256,uint256),bytes,bytes)
      UNISWAP_SELECTOR = "0x0502b1c5" #unoswap(address,uint256,uint256,uint256[])
      UNISWAPV3_SWAP_TO_SELECTOR = "0xe449022e" #uniswapV3Swap(address,uint256,uint256,uint256[])
      RFQCOMPACT_SELECTOR = "0x9570eeee" #fillOrderRFQCompact((uint256,address,address,address,address,uint256,uint256),bytes32,bytes32,uint256)

      result = get_1inch_swap_data(
        from_token=from_token.lower(),
        to_token=to_token.lower(),
        swap_amount=from_amount,
        slippage=slippage,
        from_address=seller_address,
        to_address=seller_address,
      )

      input_decoded = router_1inch.decode_input(result.input)
      selector = result.input[:10]
      #print("Input decoded: {}".format(input_decoded))
      print("Selector: {}".format(selector))
      print("Swapping : {}".format(input_decoded[0]))

      # Swap selector
      if selector == SWAP_SELECTOR:
        #swap(address,(address,address,address,address,uint256,uint256,uint256),bytes,bytes)
        tx = router_1inch.swap(
          input_decoded[1][0],
          input_decoded[1][1],
          input_decoded[1][2],
          input_decoded[1][3],
          {"from": seller_address, "value": 0, "gas_price": gas_price})
          
      elif selector == UNISWAP_SELECTOR :
        #unoswapTo(address,uint256,uint256,uint256[])
        tx = router_1inch.unoswap(
          input_decoded[1][0],
          input_decoded[1][1],
          input_decoded[1][2],
          input_decoded[1][3],
          {"from": seller_address, "value": 0, "gas_price": gas_price})
      elif selector == UNISWAPV3_SWAP_TO_SELECTOR :
        #uniswapV3SwapTo(address,uint256,uint256,uint256[])
        tx = router_1inch.uniswapV3Swap(
          input_decoded[1][0],
          input_decoded[1][1],
          input_decoded[1][2],
          {"from": seller_address, "value": 0, "gas_price": gas_price})
      elif selector == RFQCOMPACT_SELECTOR :
        #fillOrderRFQCompact((uint256,address,address,address,address,uint256,uint256),bytes32,bytes32,uint256)
        tx = router_1inch.fillOrderRFQCompact(
          input_decoded[1][0],
          input_decoded[1][1],
          input_decoded[1][2],
          input_decoded[1][3],
          {"from": seller_address, "value": 0, "gas_price": gas_price})
        
      sum = 0   
      for event in tx.events:
        if event.name == "Transfer":
            address_keys = ['to', '_to', 'dst', '_dst', 'receiver', '_receiver']
            amount_keys = ['amount', '_amount', 'value', '_value']

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

            if to_address.lower() == seller_address.lower() and amount is not None:
                sum += amount
                print(f"New sum: {sum}")

      return sum
    except Exception as e:
      logger.error(f"An error occurred: {e}")
      logger.error(traceback.format_exc())
      return 0

def print_trace(tx_receipt):
    print("Transaction Call Trace:")
    formatted_json = []
    for step in tx_receipt.trace:
      if 'input' in step and step['input'] is not None and step['input'] != '':
        print("Contract: {} : {}".format(step['to'], step['input']))
      if 'error' in step and step['error'] is not None and step['error'] != '':
        print("Error: {}".format(step['error']))
        

