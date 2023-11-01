import traceback
import os 
from scripts.frontier import update_history
from scripts.tools.constants import PRODUCTION_DATABASE,logger

try:
    db_url = PRODUCTION_DATABASE
    table_name = 'frontier_markets_history'
    
    update_history(db_url,table_name)

except Exception as e:
    logger.error(traceback.format_exc())