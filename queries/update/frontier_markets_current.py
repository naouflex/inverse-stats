import traceback
import os 
from scripts.frontier import create_current
import logging
from scripts.tools.constants import PRODUCTION_DATABASE

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[logging.StreamHandler()])

logger = logging.getLogger(__name__)

try:
    db_url = PRODUCTION_DATABASE
    table_name = 'frontier_markets_current'
    
    create_current(db_url,table_name)

except Exception as e:
    logger.error(traceback.format_exc())