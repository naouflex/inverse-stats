import traceback
import os 
from dotenv import load_dotenv
from scripts.defillama_prices import create_current
from scripts.tools.constants import PRODUCTION_DATABASE,logger

try:
    db_url = PRODUCTION_DATABASE
    table_name = 'defillama_prices_current'

    create_current(db_url,table_name)


except Exception as e:
    print(traceback.format_exc())