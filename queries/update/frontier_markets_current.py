import traceback
import os 
from scripts.frontier import create_current
from scripts.tools.constants import PRODUCTION_DATABASE,logger


try:
    db_url = PRODUCTION_DATABASE
    table_name = 'frontier_markets_current'
    
    create_current(db_url,table_name)

except Exception as e:
    logger.error(traceback.format_exc())