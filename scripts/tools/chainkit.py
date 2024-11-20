import pandas as pd

from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv

load_dotenv()

def datetime_to_unixtimestamp(dt):
    dt_utc = dt.astimezone(tz=timezone.utc)
    dt_utc_zero_time = dt_utc.replace(hour=0, minute=0, second=0, microsecond=0)
    unix_timestamp = int(dt_utc_zero_time.timestamp())
    
    return unix_timestamp


def binary_search_first_block(w3, low, high, target_timestamp):
    closest_block = None
    while low <= high:
        mid = (low + high) // 2
        mid_block = w3.eth.getBlock(mid)
        if mid_block['timestamp'] < target_timestamp:
            closest_block = mid
            low = mid + 1
        elif mid_block['timestamp'] > target_timestamp:
            high = mid - 1
        else:
            return mid + 1
    
    return closest_block + 1 if closest_block is not None else None


def first_block_after_midnight(w3, target_timestamp):
    latest_block = w3.eth.getBlock('latest')['number']
    earliest_block = w3.eth.getBlock('earliest')['number']

    return binary_search_first_block(w3, earliest_block, latest_block, target_timestamp)


def find_block_for_date(w3,current_date, df_list):
    midnight_timestamp = datetime_to_unixtimestamp(current_date)

    block_after_midnight = first_block_after_midnight(w3, midnight_timestamp)
    if block_after_midnight is not None:
        block_after_midnight = int(block_after_midnight)
    
    df_list.append({
        'timestamp': midnight_timestamp,
        'block_number': block_after_midnight
    })


def get_blocks_by_date_range(w3,start_date, end_date=None):
    if not w3.isConnected():
        logger.info("Not connected to Ethereum node.")
        return None
    if type(start_date) == str:
        start_date = datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S')
        start_date = start_date.replace(tzinfo=timezone.utc)
    else:
        if start_date.tzinfo is None:
            start_date = start_date.replace(tzinfo=timezone.utc)

    if type(end_date) == str:
        end_date = datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S')
        end_date = end_date.replace(tzinfo=timezone.utc)
    else:
        if end_date.tzinfo is None:
            end_date = end_date.replace(tzinfo=timezone.utc)

    df_list = []
    
    if  start_date > end_date or((start_date.year == end_date.year and start_date.month == end_date.month and start_date.day == end_date.day)):
        date_list = [start_date]
    else:
        date_list = [start_date + timedelta(days=x) for x in range(0, (end_date - start_date).days + 1)]

    with ThreadPoolExecutor(max_workers=12) as executor:
        for current_date in date_list:
            executor.submit(find_block_for_date, w3, current_date, df_list)

    df = pd.DataFrame(df_list)

    df = df.astype({'block_number': 'Int64'})
    df.loc[df['block_number'] <= 1, 'block_number'] = None
    
    return df

def get_blocks_for_date(w3, date):
    midnight_timestamp = datetime_to_unixtimestamp(date)

    block_after_midnight = first_block_after_midnight(w3, midnight_timestamp)
    if block_after_midnight is not None:
        block_after_midnight = int(block_after_midnight)
    
    return block_after_midnight


def get_call_result(w3,contract_address,method_name,abi,arguments,block_identifier='latest'):
    contract = w3.eth.contract(address=w3.toChecksumAddress(contract_address), abi=abi)
    call = getattr(contract.functions, method_name)
    if arguments is not None and arguments != '':
        call = call(arguments)
    else:
        call = call()
        
    return call.call(block_identifier=block_identifier)






