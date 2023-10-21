import traceback
import os 
from dotenv import load_dotenv
from dola_lp import create_current, create_history, update_history
# dola_lp dola_supply frontier
try:
    db_url = os.getenv('PROD_DB')
    table_name = 'lp_current'
    
    create_current(db_url,table_name)

except Exception as e:
    print(traceback.format_exc())