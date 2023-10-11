from blocks_daily import update_block_table, create_block_table
from datetime import datetime
from dotenv import load_dotenv 
import os 

load_dotenv()

if __name__ == "__main__":
    db_url = os.getenv('PROD_DB')
    start_time = datetime.now() 
    update_block_table(db_url,'blocks_daily')
    print(f"Time elapsed: {datetime.now() - start_time}")