from web3 import Web3
from web3.middleware import geth_poa_middleware
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
        'date': midnight_timestamp,
        'block_number': block_after_midnight
    })


def get_blocks_by_date_range(w3,start_date, end_date):
    if not w3.isConnected():
        print("Not connected to Ethereum node.")
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


def get_call_result(w3,contract_address,method_name,abi,arguments,block_identifier='latest'):
    w3.middleware_onion.inject(geth_poa_middleware, layer=0)
    contract = w3.eth.contract(address=contract_address, abi=abi)
    call = getattr(contract.functions, method_name)(*arguments)
    return call.call(block_identifier=block_identifier)


def get_pool_tokens_balancer(w3,contract_address,arguments,index,sub_index):
    w3.middleware_onion.inject(geth_poa_middleware, layer=0)
    contract = w3.eth.contract(address=contract_address, abi=abi)
    call = getattr(contract.functions, 'getPoolTokens')(*arguments)
    return call.call()[index][sub_index]

def get_custom_state(w3,abi,string,block_identifier):
    # string is a string object eg 0x2b34548b865ad66A2B046cb82e59eE43F75B90fd:globalSupply() or 0xba12222222228d8ba445958a75a0704d566bf2c8:getPoolTokens(0x8bc65eed474d1a00555825c91feab6a8255c2107000000000000000000000453)
    # we first extract the contract address and the method name
    # then we get the arguments from the string
    # then we call the method
    # then we return the result
    contract_address = string.split(':')[0]
    method_name = string.split(':')[1].split('(')[0]
    arguments = string.split(':')[1].split('(')[1][:-1].split(',')

    # if no arguments, then we call the method directly
    if len(arguments) == 1 and arguments[0] == '':
        return get_call_result(w3,contract_address,method_name,abi,[],block_identifier)
    # if len is 42 then we have a single argument and convert it to a checksum address
    elif len(arguments) == 1 and len(arguments[0]) == 42:
        return get_call_result(w3,contract_address,method_name,abi,[arguments[0]],block_identifier)
    # if len is 66 then we have a single argument and convert it to a checksum address
    elif len(arguments) == 1 and len(arguments[0]) == 66:
        return get_call_result(w3,contract_address,method_name,abi,[Web3.toChecksumAddress(arguments[0])] ,block_identifier)







