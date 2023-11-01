import traceback
import os 
from dotenv import load_dotenv
from scripts.defillama_prices import update_history
from scripts.tools.constants import PRODUCTION_DATABASE,logger

try:
    db_url = PRODUCTION_DATABASE
    table_name = 'defillama_prices'

    update_history(db_url,table_name)


except Exception as e:
    print(traceback.format_exc())