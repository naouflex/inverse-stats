import json
import subprocess
import time
from brownie import  network, accounts, Contract
import os
from dotenv import load_dotenv
from web3 import Web3
load_dotenv()


def main():
    try: 
        owner = '0x926dF14a23BE491164dCF93f4c468A50ef659D5B'
        curve_pool_address = '0x5426178799ee0a0181A89b4f57eFddfAb49941Ec'
        sell_token_address = '0x41D5D79431A913C4aE7d69a668ecdfE5fF9DFB68'
        sell_index = int(2)
        output_index = int(0)
        swap_amount = int(1)
        
        with open('./token_abi.json') as f:
            token_abi = json.load(f)
        

        w3 = Web3(Web3.HTTPProvider('http://127.0.0.1:8545'))
        #w3 = Web3(Web3.HTTPProvider(os.getenv('RPC_URL')))
        contract = w3.eth.contract(address=curve_pool_address, abi=token_abi)

        print(contract.functions.totalSupply().call())
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if network.is_connected():
            network.disconnect()

if __name__ == "__main__":
    main()



