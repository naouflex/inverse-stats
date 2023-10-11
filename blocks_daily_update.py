import requests
import pandas as pd
import os
from datetime import datetime, timedelta
from web3 import Web3
from web3.middleware import geth_poa_middleware
from tools.chainkit import get_blocks_by_date_range
from tools.database import save_table, get_table,update_table
from datetime import datetime
from dotenv import load_dotenv  
import logging

logger = logging.getLogger(__name__)

load_dotenv()

def get_rpc_table():
    try:
        web3_providers = requests.get(os.getenv('WEB3_PROVIDERS')).json()
        web3_providers = pd.DataFrame(web3_providers['query_result']['data']['rows']) 
        return web3_providers
    except Exception as e:
        logger.error(f"Cannot get rpc table: {e}")
        return

def update_block_table(db_url,table_name,end_date=None):

    try:
        # get block table to get the last date in the db
        block_table = get_table(db_url,table_name)
        
        last_date = block_table['date'].max()

        # convert unix timestamp to datetime
        last_date = datetime.fromtimestamp(last_date)

        # if lastdate is today we exit
        if last_date.year == datetime.now().year and last_date.month == datetime.now().month and last_date.day == datetime.now().day:
            logger.warning("Block table is up to date")
            return
        
        last_date = last_date + timedelta(days=1)

        if end_date is None:
            end_date = datetime.now().strftime("%Y-%m-%d 00:00:00")
        
        web3_providers = get_rpc_table()

        for i in range(len(web3_providers)):
            w3 = Web3(Web3.HTTPProvider(web3_providers['rpc_url'][i]))
            w3.middleware_onion.inject(geth_poa_middleware, layer=0)
            chain_id = web3_providers['chain_id'][i]
            chain_name = web3_providers['chain_name'][i]

            chain_blocks = get_blocks_by_date_range(w3,last_date, end_date)
            
            if i==0:
                chain_blocks = chain_blocks.rename(columns={'block_number': chain_name})
                block_table = chain_blocks
            else:
                #rename block_number to chain_name_block_number and left merge to block_table
                chain_blocks = chain_blocks.rename(columns={'block_number': chain_name})
                block_table = pd.merge(block_table, chain_blocks, on='date', how='left')
            logger.info(f"chain_id: {chain_id}, chain_name: {chain_name} processed")
        block_table = pd.DataFrame(block_table)
        update_table(db_url,table_name,block_table)
        return
    except Exception as e:
        logger.error(f"Cannot update block table: {e}")
        return
    
def run():
    db_url = os.getenv('PROD_DB')
    start_time = datetime.now() 
    update_block_table(db_url,'blocks_daily')
    logger.info(f"Time elapsed: {datetime.now() - start_time}")