import traceback
from defillama_prices_create import run

try:
    run()
except Exception as e:
    print(traceback.format_exc())