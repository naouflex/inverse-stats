import traceback
import os 
from dotenv import load_dotenv
from scripts.blocks_daily import update_history
from scripts.tools.constants import PRODUCTION_DATABASE,logger

try:
    db_url = PRODUCTION_DATABASE
    table_name = 'blocks_daily_test2'
    
    update_history(db_url,table_name)


except Exception as e:
    print(traceback.format_exc())