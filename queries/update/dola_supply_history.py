import traceback
import os 
from scripts.dola_supply import update_history
import logging

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[logging.StreamHandler()])

try:
    db_url = os.getenv('PROD_DB')
    table_name = 'dola_supply_history'
    
    update_history(db_url,table_name)

except Exception as e:
    print(traceback.format_exc())