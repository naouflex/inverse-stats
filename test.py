import traceback
from blocks_daily import create_current, update_current, update_history

try:
    #create_current()
    update_current()
    #create_current()
    #update_history()

except Exception as e:
    print(traceback.format_exc())