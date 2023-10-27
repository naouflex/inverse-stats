import traceback
import os 
from dola_lp import update_history
import logging

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[logging.StreamHandler()])

try:
    db_url = os.getenv('PROD_DB')
    
    table_name = 'lp_history'
    update_history(db_url,table_name)

except Exception as e:
    print(traceback.format_exc())