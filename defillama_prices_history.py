import traceback
import os 
from dotenv import load_dotenv
from scripts.defillama_prices import create_current, create_history
from scripts.tools.constants import PRODUCTION_DATABASE,logger

try:
    db_url = PRODUCTION_DATABASE
    table_name = 'defillama_prices_ghuy'

    create_current(db_url,table_name)

except Exception as e:
    logger.info(traceback.format_exc())