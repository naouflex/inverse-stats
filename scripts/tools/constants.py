import os
from dotenv import load_dotenv
import logging


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[logging.StreamHandler()])

logger = logging.getLogger(__name__)

load_dotenv()

CHAIN_ID_MAP = {
    'ethereum': 1, 'bsc': 56, 'polygon': 137, 'fantom': 250, 
    'optimism': 10, 'arbitrum': 42161, 'avax': 43114
}

PRICE_METHODOLOGY = "https://app.inverse.watch/api/queries/480/results.json?api_key=JY9REfUM3L7Ietj76qmQ2wFioz7k6GdCL6YqRxHG"
LP_METHODOLOGY_URL = "https://app.inverse.watch/api/queries/479/results.json?api_key=zCljA8HpUclyQQ4xHH3mpIaCBhjtjf2ljTd77Y9V"
SUPPLY_METHODOLOGY_URL = "https://app.inverse.watch/api/queries/492/results.json?api_key=8eH4yNtb6tYYbTWHskb5a4MrLbeccV93NdbFcg64"
FRONTIER_METHODOLOGY_URL = "https://app.inverse.watch/api/queries/499/results.json?api_key=hPTTHXRhBI36YiK4UYrYzFA481GheipJLJ1ubIOA"
MCAP_METHODOLOGY_URL = "https://app.inverse.watch/api/queries/547/results.json?api_key=6vNIRr3Im4IFKmyweEX0uTBTJvWe4lQpItmjrdXp"
TREASURY_METHODOLOGY_URL = "https://app.inverse.watch/api/queries/548/results.json?api_key=XiugsXqyRbVuqhltHfUHteOLMsXYDEZd4ESHwpoI"

WEB3_PROVIDERS_URL = "https://app.inverse.watch/api/queries/467/results.json?api_key=vX3BktSCzAW4jzpjeR3hvqjAAJ2zJb5dXAxwaqCa"
PRODUCTION_DATABASE = os.getenv("PROD_DB")