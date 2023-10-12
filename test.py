import traceback
from defillama_prices import create_current, update_current, create_history, update_history

try:
    #create_current()
    #update_current()
    create_history()
    #update_history()

except Exception as e:
    print(traceback.format_exc())