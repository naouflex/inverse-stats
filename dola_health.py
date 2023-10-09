import requests
import pandas as pd
import traceback
from web3 import Web3
from web3.middleware import geth_poa_middleware
import os

def get_methodology_dola_health():
    try:
        methodology = requests.get("https://app.inverse.watch/api/queries/466/results.json?api_key=rr0yOmZp1wtw1owcQavb73QnAhHWJG356YX6Hj38").json()
        web3_providers = requests.get(os.getenv("WEB3_PROVIDERS")).json()
        smart_contracts = requests.get("https://app.inverse.watch/api/queries/454/results.json?api_key=CNsPQor5gykZdi7jS746PngKK5M8KGeZsGvOZZPf").json()

        methodology = pd.DataFrame(methodology['query_result']['data']['rows'])
        web3_providers = pd.DataFrame(web3_providers['query_result']['data']['rows']) 
        smart_contracts = pd.DataFrame(smart_contracts['query_result']['data']['rows'])

        for i in range(len(web3_providers)):
            w3 = Web3(Web3.HTTPProvider(web3_providers['rpc_url'][i]))
            w3.middleware_onion.inject(geth_poa_middleware, layer=0)
            web3_providers.loc[i, 'last_block_number'] = w3.eth.blockNumber

        full_methodology = pd.merge(methodology, web3_providers, on='chain_id', how='left')

        print(full_methodology)
        return full_methodology
    except Exception as e:
        print(f"Error in getting methodology : {e}")
        traceback.print_exc()
        return None

def get_rpc_table():
    try:
        web3_providers = requests.get("").json()
        web3_providers = pd.DataFrame(web3_providers['query_result']['data']['rows']) 
        return web3_providers
    except Exception as e:
        print(f"Cannot get rpc table: {e}")
        return
    
def create_db_table_current():
    return

def update_db_table_current():
    return

def create_db_table_history():
    return

def update_db_table_history():
    return

def process_contract(i, full_data, result_state, result_event):
    try:
        contract_start_time = datetime.now()

        start_block = int(full_data['start_block'][i])
        end_block = int(full_data['last_block_number'][i])
        lag = int(full_data['lag'][i])

        w3 = Web3(Web3.HTTPProvider(full_data['rpc_url'][i]))
        w3.middleware_onion.inject(geth_poa_middleware, layer=0)
        contract = w3.eth.contract(address=Web3.toChecksumAddress(full_data['call_contract_address'][i]), abi=(full_data['abi'][i]))

        if full_data['type'][i] == 'state':
            for block in range(start_block, end_block, lag):
                function = getattr(contract.functions, full_data['function'][i])
                args = full_data['args'][i]
                if full_data['account'][i] == 'fed':
                    try:
                        # Value is the function call or 0 if the function call fails
                        value = function().call(block_identifier=block)
                        if value is not None and block is not None:
                            result_state.append({'block_number': block,
                                                'block_timestamp': w3.eth.getBlock(block)['timestamp'],
                                                'contract_address': "0x865377367054516e17014ccded1e7d814edc9ce4",
                                                'function': full_data['function'][i], 
                                                'args': full_data['call_contract_address'][i], 
                                                'result': value})
                    except Exception as e:
                        pass
                elif full_data['account'][i] == 'frontier':
                    try:
                        value = function().call(block_identifier=block)
                        if value is not None and block is not None:
                            result_state.append({'block_number': block,
                                                'block_timestamp': w3.eth.getBlock(block)['timestamp'],
                                                'contract_address': full_data['collateral'][i],
                                                'function': full_data['function'][i], 
                                                'args': full_data['call_contract_address'][i], 
                                                'result': value})
                    except Exception as e:
                        pass
                elif full_data['account'][i] == 'lp':
                    if full_data['function'][i] == 'getPoolTokens':                
                        try:
                            value = function(args).call(block_identifier=block)
                            if value is not None and block is not None:
                                for j in value[0]:
                                    result_state.append({'block_number': block,
                                                        'block_timestamp': w3.eth.getBlock(block)['timestamp'],
                                                        'contract_address': value[0][value[0].index(j)], 
                                                        'function': full_data['function'][i], 
                                                        'args': args[0:42], 
                                                        'result': value[1][value[0].index(j)]})
                        except Exception as e:
                            pass
                    elif full_data['function'][i] == 'balanceOf':
                        try:
                            value = function(args).call(block_identifier=int(block))
                            if value is not None and block is not None:
                                if args[0:2] == '0x' and len(args) == 42:
                                    args = Web3.toChecksumAddress(args)
                                result_state.append({'block_number': block,
                                                    'block_timestamp': w3.eth.getBlock(block)['timestamp'],
                                                    'contract_address': full_data['call_contract_address'][i], 
                                                    'function': full_data['function'][i], 
                                                    'args': args, 
                                                    'result': value})
                        except Exception as e:
                            pass

        if full_data['type'][i] == 'event':
            start_block = int(full_data['start_block'][i])
            end_block = int(full_data['last_block_number'][i])
            lag = int(full_data['lag'][i])
            blocks = range(start_block, end_block, lag)

            for block in blocks:
                event_filter = contract.events[full_data['function'][i]].createFilter(fromBlock=block, toBlock=block+lag)
                events = event_filter.get_all_entries()
                for event in events:
                    if full_data['function'][i] in ['Withdraw', 'Repay']:
                        result_event.append({'block_number': event['blockNumber'],
                                             'block_timestamp': w3.eth.getBlock(block)['timestamp'],
                                             'contract_address': full_data['collateral'][i],
                                             'function': full_data['function'][i],
                                             'args': full_data['call_contract_address'][i],
                                             'result': decimal.Decimal(event['args']['amount']) * -1})
                    elif full_data['function'][i] in ['Deposit', 'Borrow']:
                        result_event.append({'block_number': event['blockNumber'],
                                            'block_timestamp': w3.eth.getBlock(block)['timestamp'],
                                            'contract_address': full_data['collateral'][i],
                                            'function': full_data['function'][i],
                                            'args': full_data['call_contract_address'][i],
                                            'result': decimal.Decimal(event['args']['amount'])})

        contract_end_time = datetime.now()
        print(f"contract {full_data['name'][i]} - {full_data['call_contract_address'][i]} - {full_data['start_block'][i]} to {full_data['last_block_number'][i]} -  execution time {contract_end_time - contract_start_time}")

    except Exception as e:
        print(e)

try:
    start_time = datetime.now()
    
    # Fetch query results from API endpoints
    json_data = requests.get("https://app.inverse.watch/api/queries/466/results.json?api_key=rr0yOmZp1wtw1owcQavb73QnAhHWJG356YX6Hj38").json()
    web3_providers = requests.get().json()
    smart_contracts = requests.get("https://app.inverse.watch/api/queries/454/results.json?api_key=CNsPQor5gykZdi7jS746PngKK5M8KGeZsGvOZZPf").json()
    
    df_data = pd.DataFrame(json_data['query_result']['data']['rows'])
    web3_providers = pd.DataFrame(web3_providers['query_result']['data']['rows']) 
    smart_contracts = pd.DataFrame(smart_contracts['query_result']['data']['rows'])
    
    # Mount web3 clients and add last_block_number to df_data
    for i in range(len(web3_providers)):
        w3 = Web3(Web3.HTTPProvider(web3_providers['rpc_url'][i]))
        w3.middleware_onion.inject(geth_poa_middleware, layer=0)
        web3_providers.loc[i, 'last_block_number'] = w3.eth.blockNumber
    
    # Merge df_data and web3_providers on chain_id
    full_data = pd.merge(df_data, web3_providers, on='chain_id', how='left')
    
    # Break down lines on call contract address if there are several addresses separated by a comma
    full_data = full_data.assign(call_contract_address=full_data.call_contract_address.str.split(',')).explode('call_contract_address')
    # Break down lines on function if there are several functions separated by a comma
    full_data = full_data.assign(function=full_data.function.str.split(',')).explode('function')
    # Break down lines on args if there are several args separated by a comma
    full_data = full_data.assign(args=full_data.args.str.split(',')).explode('args')
    
    # Merge full_data and smart_contracts on contract_address in lowercase
    full_data['call_contract_address'] = full_data['call_contract_address'].str.lower()
    smart_contracts['call_contract_address'] = smart_contracts['address'].str.lower()
    
    # Left join full_data and smart_contracts on contract_address
    full_data = pd.merge(full_data, smart_contracts, on='call_contract_address', how='left')
    
    # Create empty lists for results
    result_state = []
    result_event = []
    
    # Process contracts in separate threads
    contract_data_list = []
    for i in range(len(full_data)):
        contract_data_list.append((i, full_data, result_state, result_event))
    
    threads = []
    
    # We want 10 threads at most concurrently running; when one stops, another one starts
    for contract_data in contract_data_list:
        t = threading.Thread(target=process_contract, args=contract_data)
        threads.append(t)
        t.start()
        while threading.active_count() > 30:
            pass
    
    for thread in threads:
        thread.join()
    
    # Convert results to DataFrames and save to CSV
    result_state = pd.DataFrame(result_state)
    result_event = pd.DataFrame(result_event)
    
    # convert result to Decimal
    
    for i in range(len(result_event)):
        result_event.at[i, 'result'] = decimal.Decimal(result_event.at[i, 'result'])
    
    # Sort by block_number
    result_event = result_event.sort_values(by=['block_number'])
    
    final_event = result_event[['block_number','block_timestamp','contract_address']].drop_duplicates()
    
    # totalBorrows is the sum of all borrow and repay function for the blocks prior to the current block
    final_event['totalBorrows'] = decimal.Decimal(0)
    # collateral is the sum of all deposit and withdraw function for the blocks prior to the current block
    final_event['collateral'] = decimal.Decimal(0)
    # Reinitialize index
    final_event = final_event.reset_index(drop=True)
    
    # Calculate totalBorrows and collateral
    for i in range(len(final_event)):
        # Get contract address
        contract_address = final_event.iloc[i]['contract_address']
    
        block_number = final_event.iloc[i]['block_number']
        
        # Get all borrow and repay function for the blocks prior to the current block in result_event (result is a big Int)
        totalBorrows = decimal.Decimal(result_event[(result_event['contract_address'] == contract_address) & (result_event['block_number'] <= block_number) & (result_event['function'].isin(['Borrow','Repay']))]['result'].sum())
        
        # Get all deposit and withdraw function for the blocks prior to the current block
        collateral = decimal.Decimal(result_event[(result_event['contract_address'] == contract_address) & (result_event['block_number'] <= block_number) & (result_event['function'].isin(['Deposit','Withdraw']))]['result'].sum())
        
        # Update totalBorrows and collateral, avoid OverflowError: Python int too large to convert to C long
        final_event.at[i, 'totalBorrows'] = totalBorrows
        final_event.at[i, 'collateral'] = collateral

    
    # Turn totalBorrows and collateral columns into two rows with 'function' column where function is 'totalBorrows' and 'collateral'
    final_event = final_event.melt(id_vars=['block_number','block_timestamp','contract_address'], value_vars=['totalBorrows','collateral'], var_name='function', value_name='result')
    
    # Add args column after function
    final_event.insert(3, 'args', '')
    
    # Put result_state and final_event on top of each other
    result = pd.concat([result_state, final_event])

    
    # Put in redash format with rows and columns
    result = {
    "columns": [
        {"name": "block_number", "type": "integer"},
        {"name": "block_timestamp", "type": "integer"},
        {"name": "contract_address", "type": "string"},
        {"name": "function", "type": "string"},
        {"name": "args", "type": "string"},
        {"name": "result", "type": "string"}
    ],
    "rows": result[['block_number','block_timestamp','contract_address','function','args','result']].to_dict('records')
    }
    
    print(f"Total execution time: {datetime.now() - start_time}")
    
except Exception as e:
    print(traceback.format_exc())


