import requests
import pandas as pd
import json
from scripts.tools.common import df_to_result

# Create a dictionary where the keys are the addresses and the values are the labels
addresses = {
    "0x926df14a23be491164dcf93f4c468a50ef659d5b": "treasury",
    "0x9D5Df30F475CEA915b1ed4C0CCa59255C897b61B": "mainnet twg",
    "0xa283139017a2f5BAdE8d8e25412C600055D318F8": "opti twg",
    "0xF7Da4bC9B7A6bB3653221aE333a9d2a2C2d5BdA7": "bnb twg",
    "0x5D18b089e838DFFbb417A87874435175F3A9B000": "poly twg",
    "0x586CF50c2874f3e3997660c0FD0996B090FB9764": "base twg",
    "0x23dEDab98D7828AFBD2B7Ab8C71089f2C517774a": "arbi twg"
}

headers = {
    "accept": "application/json",
    "authorization": "Basic emtfZGV2X2E3YmY0NzExMjhiNjQxNzE4MWY5ZWY0ZmNjZTNjZTE1Og=="
}

all_data = []

requests_sent = 0
request_limit = {{REQUEST LIMIT}}

# Iterate over the items in the dictionary
for address, label in addresses.items():
    #print("Extracting", address, label)
    url = f"https://api.zerion.io/v1/wallets/{address}/transactions/?currency=usd&page[size]=100&filter[trash]=only_non_trash"

    while url  and (request_limit == None or requests_sent < request_limit):
        try:
            response = requests.get(url, headers=headers)
            requests_sent += 1
            
            json_response = response.json()
            
            url = json_response['links'].get('next')

            # Convert the 'data' key in the JSON object into a pandas DataFrame
            df = pd.json_normalize(json_response['data'])
            
            # If the 'data' column exists and contains nested data, normalize it
            if 'data' in df.columns and isinstance(df['data'].iloc[0], dict):
                df_data = pd.json_normalize(df['data'])
                df = pd.concat([df.drop('data', axis=1), df_data], axis=1)
    
            if 'attributes.transfers' in df.columns:
                # we cant have one or several transfers so we need to duplicate the row for each transfer
                df = df.explode('attributes.transfers')
                df_transfers_data = pd.json_normalize(df['attributes.transfers'])
                df.reset_index(drop=True, inplace=True)
                df_transfers_data.reset_index(drop=True, inplace=True)
                df = pd.concat([df.drop('attributes.transfers', axis=1), df_transfers_data], axis=1)

    
            # Add a new column to the dataframe that contains the label for the address
            df['id_name'] = label
    
            all_data.append(df)

        except Exception as e:
            print(f"An error occurred: {e}")
            url = None
            continue

try:        
    if all_data:
        # Concatenate all dataframes
        all_data_df = pd.concat(all_data, ignore_index=True)

        all_columns = all_data_df.columns

        try:
            columns = [
                'attributes.mined_at',
                'attributes.mined_at_block',
                'id',
                'fungible_info.icon.url',
                'relationships.dapp.data.id',
                'relationships.chain.data.id',
                'attributes.operation_type',
                'attributes.hash',
                'attributes.sent_from',
                'attributes.sent_to',
                'attributes.status',
                'attributes.fee.fungible_info.symbol',
                'attributes.fee.quantity.float',
                'attributes.fee.price',
                'attributes.fee.value',
                #'fungible_info.implementations',
                'sender',
                'recipient',
                'direction',
                'fungible_info.symbol',
                'quantity.float',
                'price',
                'value',
            ]
            columns_to_keep =[]
            
            for col in columns:
                if col in all_data_df.columns:
                    columns_to_keep.append(col)
            
            all_data_df = all_data_df[columns_to_keep]

            #add a column with the sum of all 'value' since the earliest mined_at substracting out and adding in direction
            all_data_df['value'] = all_data_df['value'].fillna(0)
            all_data_df['value'] = all_data_df['value'].astype(float)
            all_data_df['value'] = all_data_df['value'] * all_data_df['direction'].apply(lambda x: -1 if x == 'out' else 1)

            # add a cumulated sum of the value ordered by mined_at
            all_data_df['cumulated_value'] = all_data_df.sort_values('attributes.mined_at')['value'].cumsum()

            #order by mined_at descending
            all_data_df = all_data_df.sort_values('attributes.mined_at', ascending=False)
            
        except Exception as e:
            print(f"An error occurred while filtering columns: {e}")

        result = df_to_result(all_data_df)
    else:
        result = 'No data found'
        
except Exception as e:
    result = f"No data found due to error: {e}"

#print(result)
#print("All columns:", all_columns)
print(f"Requests sent: {requests_sent}")
