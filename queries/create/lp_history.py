import traceback
import os 
from scripts.dola_lp import create_current, create_history
from scripts.tools.constants import PRODUCTION_DATABASE,logger

try:
    db_url = PRODUCTION_DATABASE
    table_name = 'lp_history'
    
    create_history(db_url,table_name)


except Exception as e:
    print(traceback.format_exc())