import traceback
import os 
from dotenv import load_dotenv
from dola_lp import create_current, create_history
# dola_lp dola_supply frontier defillama_prices
import logging

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[logging.StreamHandler()])

try:
    db_url = os.getenv('PROD_DB')
    #table_name = 'lp_current_test'
    table_name = 'lp_history'
    #table_name = 'dola_supply_current'
    #table_name = 'dola_supply_history'
    #table_name = 'frontier_markets_current'
    #table_name = 'frontier_markets_history'
    #table_name = 'defillama_prices'
    #table_name = 'defillama_prices_current'
    #table_name = 'defillama_prices'
    #table_name = 'fuse_current'
    #table_name = 'dola_supply_history'
    #create_current(db_url,table_name)
    
    create_history(db_url,table_name)

except Exception as e:
    print(traceback.format_exc())