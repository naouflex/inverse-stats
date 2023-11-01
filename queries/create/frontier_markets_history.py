import traceback
import os 
from dotenv import load_dotenv
from scripts.frontier import create_current, create_history
from scripts.tools.constants import PRODUCTION_DATABASE,logger

try:
    db_url = PRODUCTION_DATABASE
    table_name = 'frontier_markets_history'
    
    create_history(db_url,table_name)


except Exception as e:
    print(traceback.format_exc())