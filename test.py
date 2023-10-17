import traceback
import os 
from dotenv import load_dotenv
from dola_supply import create_current
load_dotenv()

try:
    db_url = os.getenv('PROD_DB')
    table_name = 'dola_supply_current'
    
    create_current(db_url,table_name)

except Exception as e:
    print(traceback.format_exc())