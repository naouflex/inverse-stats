import traceback
import os 
from scripts.blocks_daily import create_current, create_history,update_history
from scripts.tools.constants import PRODUCTION_DATABASE,logger

try:
    db_url = PRODUCTION_DATABASE
    table_name = 'blocks_daily'
    
    create_history(db_url,table_name,'2020-01-01 00:00:00')

except Exception as e:
    logger.error(traceback.format_exc())