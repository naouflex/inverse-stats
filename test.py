import traceback
import os 
from dotenv import load_dotenv
from defillama_prices import create_history
load_dotenv()

try:
    db_url = os.getenv('PROD_DB')
    table_name = 'defillama_prices'
    
    create_history(db_url,table_name)

except Exception as e:
    print(traceback.format_exc())