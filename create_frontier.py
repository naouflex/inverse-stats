import traceback
import os 
from dotenv import load_dotenv
from frontier import create_current, create_history,update_history
import logging

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[logging.StreamHandler()])

try:
    db_url = os.getenv('PROD_DB')
    #table_name = 'frontier_markets_history'
    
    #update_history(db_url,table_name)

    table_name = 'frontier_markets_current'
    create_current(db_url,table_name)

except Exception as e:
    print(traceback.format_exc())