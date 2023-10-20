import traceback
import os 
from dotenv import load_dotenv
from dola_supply import create_current, create_history, update_history

try:
    db_url = os.getenv('PROD_DB')
    table_name = 'dola_supply_current_2'
    
    create_current(db_url,table_name)

except Exception as e:
    print(traceback.format_exc())