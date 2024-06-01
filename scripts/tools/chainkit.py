import pandas as pd
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv
from scripts.tools.constants import logger
import os

load_dotenv()

def datetime_to_unixtimestamp(dt):
    dt_utc = dt.astimezone(tz=timezone.utc)
    dt_utc_zero_time = dt_utc.replace(hour=0, minute=0, second=0, microsecond=0)
    unix_timestamp = int(dt_utc_zero_time.timestamp())
    return unix_timestamp

def binary_search_first_block(w3, low, high, target_timestamp):
    closest_block = None
    closest_timestamp_diff = float('inf')

    while low <= high:
        mid = (low + high) // 2
        mid_block = w3.eth.getBlock(mid)
        mid_timestamp = mid_block['timestamp']
        timestamp_diff = abs(mid_timestamp - target_timestamp)

        if timestamp_diff < closest_timestamp_diff:
            closest_timestamp_diff = timestamp_diff
            closest_block = mid

        if mid_timestamp < target_timestamp:
            low = mid + 1
        elif mid_timestamp > target_timestamp:
            high = mid - 1
        else:
            return mid

    return closest_block

def first_block_after_midnight(w3, target_timestamp):
    latest_block = w3.eth.getBlock('latest')['number']
    earliest_block = 0  # Assumes earliest block is block 0, adjust if necessary

    return binary_search_first_block(w3, earliest_block, latest_block, target_timestamp)

def find_block_for_date(w3, current_date, df_list):
    midnight_timestamp = datetime_to_unixtimestamp(current_date)
    block_after_midnight = first_block_after_midnight(w3, midnight_timestamp)

    if block_after_midnight is not None:
        block_after_midnight = int(block_after_midnight)
    
    logger.info(f"Date: {current_date}, Block: {block_after_midnight}, Chain: {w3.chainId}")
    
    df_list.append({
        'timestamp': midnight_timestamp,
        'block_number': block_after_midnight
    })

def get_blocks_by_date_range(w3, start_date, end_date=None):
    if not w3.isConnected():
        return None
    
    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S')
    if start_date.tzinfo is None:
        start_date = start_date.replace(tzinfo=timezone.utc)

    if end_date:
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S')
        if end_date.tzinfo is None:
            end_date = end_date.replace(tzinfo=timezone.utc)

    df_list = []

    if end_date is None or start_date >= end_date:
        date_list = [start_date]
    else:
        date_list = [start_date + timedelta(days=x) for x in range(0, (end_date - start_date).days + 1)]

    with ThreadPoolExecutor(max_workers=1) as executor:
        for current_date in date_list:
            executor.submit(find_block_for_date, w3, current_date, df_list)

    df = pd.DataFrame(df_list)

    # Ensure 'block_number' is present in the DataFrame before applying dtype change
    if 'block_number' in df.columns:
        df = df.astype({'block_number': 'Int64'})
        df.loc[df['block_number'] <= 1, 'block_number'] = None

    return df

def get_blocks_for_date(w3, date):
    midnight_timestamp = datetime_to_unixtimestamp(date)
    block_after_midnight = first_block_after_midnight(w3, midnight_timestamp)
    if block_after_midnight is not None:
        block_after_midnight = int(block_after_midnight)
    
    return block_after_midnight

def get_call_result(w3, contract_address, method_name, abi, arguments, block_identifier='latest'):
    contract = w3.eth.contract(address=w3.toChecksumAddress(contract_address), abi=abi)
    call = getattr(contract.functions, method_name)
    if arguments:
        call = call(arguments)
    else:
        call = call()
        
    return call.call(block_identifier=block_identifier)
