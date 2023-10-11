from blocks_daily import update_block_table, create_block_table
import os
from datetime import datetime


if __name__ == "__main__":
    db_url = os.getenv('PROD_DB')
    start_time = datetime.now()
    start_date = "2020-01-01 00:00:00"  
    #end_date = "2020-01-05 00:00:00"  
    update_block_table(db_url,'blocks_daily')
    print(f"Time elapsed: {datetime.now() - start_time}")

