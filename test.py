import traceback
from defillama_prices import create_current,create_history

try:
    #create_current()
    create_history()

except Exception as e:
    print(traceback.format_exc())