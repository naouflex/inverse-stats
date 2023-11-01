import traceback
import os 
from dotenv import load_dotenv
from scripts.defillama_prices import create_current, create_history
import logging

from scripts.tools.constants import PRODUCTION_DATABASE

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[logging.StreamHandler()])

try:
    db_url = PRODUCTION_DATABASE
    table_name = 'defillama_prices'

    create_history(db_url,table_name)

except Exception as e:
    print(traceback.format_exc())