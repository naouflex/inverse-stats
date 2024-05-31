
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
from types import SimpleNamespace
from scripts.tools.one_inch import get_1inch_quote, get_1inch_swap, init_network, print_trace

load_dotenv()

ETHERSCAN_API_KEY = os.getenv('ETHERSCAN_API_KEY')

sDOLA = "0xb45ad160634c528cc3d2926d9807104fa3157305"
sDOLA_HELPER = "0x5c1f6a62cc587e135280cbd59520def551bb3c97"
DOLA = "0x865377367054516e17014ccded1e7d814edc9ce4"
DBR = "0xad038eb671c44b853887a7e32528fab35dc5d710"

# Use account 0 as the seller
TREASURY = "0x926df14a23be491164dcf93f4c468a50ef659d5b"

GAS_SRATEGY = ["50 gwei", "150 gwei", 1.1]

# Load ABIs from Etherscan
try:
  sDOLA_ABI = json.loads(requests.get(f"https://api.etherscan.io/api?module=contract&action=getabi&address={sDOLA}&apikey={ETHERSCAN_API_KEY}").json()['result'])
except:
  sDOLA_ABI = json.loads(requests.get(f"https://api.etherscan.io/api?module=contract&action=getabi&address={sDOLA}").json()['result'])
try:
  sDOLA_HELPER_ABI = json.loads(requests.get(f"https://api.etherscan.io/api?module=contract&action=getabi&address={sDOLA_HELPER}&apikey={ETHERSCAN_API_KEY}").json()['result'])
except:
  sDOLA_HELPER_ABI = json.loads(requests.get(f"https://api.etherscan.io/api?module=contract&action=getabi&address={sDOLA_HELPER}").json()['result'])
try:
  DOLA_ABI = json.loads(requests.get(f"https://api.etherscan.io/api?module=contract&action=getabi&address={DOLA}&apikey={ETHERSCAN_API_KEY}").json()['result'])
except:
  DOLA_ABI = json.loads(requests.get(f"https://api.etherscan.io/api?module=contract&action=getabi&address={DOLA}").json()['result'])
try:
  DBR_ABI = json.loads(requests.get(f"https://api.etherscan.io/api?module=contract&action=getabi&address={DBR}&apikey={ETHERSCAN_API_KEY}").json()['result'])
except:
  DBR_ABI = json.loads(requests.get(f"https://api.etherscan.io/api?module=contract&action=getabi&address={DBR}").json()['result'])

DOLA_AMOUNT = 1000

try: 
    results = pd.DataFrame()
    gas_strategy = init_network("mainnet-fork", GAS_SRATEGY)
    
    sDOLA_contract = Contract.from_abi("sDOLA", address=sDOLA, abi=sDOLA_ABI)
    sDOLA_helper_contract = Contract.from_abi("sDOLAHelper", address=sDOLA_HELPER, abi=sDOLA_HELPER_ABI)
    dola_contract = Contract.from_abi("DOLA", address=DOLA, abi=DOLA_ABI)
    dbr_contract = Contract.from_abi("DBR", address=DBR, abi=DBR_ABI)

    #transfer 1 eth to treasury
    accounts[0].transfer(TREASURY, "1 ether", gas_price=gas_strategy)

    # mint dola to seller in force mode
    dola_contract.mint(accounts[0],DOLA_AMOUNT* 10**18, {'from': accounts.at(TREASURY, force=True), 'gas_price': gas_strategy})


    #check the balance of the adress
    print(f"Balance of seller: {dola_contract.balanceOf(accounts[0])}")

    sdola_get_dbr_reserve = sDOLA_contract.getDbrReserve()
    sdola_get_dola_reserve = sDOLA_contract.getDolaReserve(sdola_get_dbr_reserve)
    numerator = DOLA_AMOUNT * 1e18 * sdola_get_dbr_reserve
    denominator = DOLA_AMOUNT * 1e18 + sdola_get_dola_reserve
    dbr_out = numerator / denominator
    print(f"Expected dbr out: {dbr_out}")

    # use swapExactDolaForDbr  on the helper
    dola_contract.approve(sDOLA_HELPER, 2**256-1, {"from": accounts[0], 'gas_price': gas_strategy})
    sDOLA_helper_contract.swapExactDolaForDbr(DOLA_AMOUNT * 1e18, dbr_out, {'from': accounts[0], 'gas_price': gas_strategy})

    #check the balance of the adress
    print(f"Balance of seller: {dola_contract.balanceOf(accounts[0])}")



except Exception as e:
    print(f"Error: {e} : {traceback.format_exc()}")
    if hasattr(e, 'txid'):
      tx_receipt = TransactionReceipt(e.txid)
      print_trace(tx_receipt)


