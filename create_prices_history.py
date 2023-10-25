import traceback
import os 
from dotenv import load_dotenv
from defillama_prices import create_current, create_history
# dola_lp dola_supply frontier defillama_prices
import logging

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[logging.StreamHandler()])

try:
    db_url = os.getenv('PROD_DB')
    table_name = 'defillama_prices'
    create_history(db_url,table_name)

except Exception as e:
    print(traceback.format_exc())