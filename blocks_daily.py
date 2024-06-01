import traceback
import os 
from scripts.blocks_daily import create_current, create_history,update_history,append_history_with_columns
from scripts.tools.constants import PRODUCTION_DATABASE,logger

try:
    db_url = PRODUCTION_DATABASE
    table_name = 'blocks_daily'
    
    append_history_with_columns(db_url,table_name, '2020-01-01 00:00:00', None,'base')

except Exception as e:
    logger.error(traceback.format_exc())