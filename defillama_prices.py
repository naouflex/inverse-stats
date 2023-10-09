import requests
import pandas as pd
from datetime import datetime, timedelta

# TODO : replace the token address list with API call, update to db instead of redash results
# token address list with blockchain
token_address_list = [
    ["avax", "0x221743dc9E954bE4f86844649Bf19B43D6F8366d"],
    ["avax", "0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E"],
    ["ethereum", "0x6c3F90f043a72FA612cbac8115EE7e52BDe6E490"],
    ["ethereum", "0x3175df0976dfa876431c2e9ee6bc45b65d3473cc"],
    ["ethereum", "0x6b175474e89094c44da98b954eedeac495271d0f"],
    ["ethereum", "0x27b5739e22ad9033bcbf192059122d163b60349d"],
    ["ethereum", "0x62b9c7356a2dc64a1969e19c23e4f579f9810aa7"],
    ["ethereum", "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"],
    ["ethereum", "0xd533a949740bb3306d119cc777fa900ba034cd52"],
    ["ethereum", "0xae7ab96520de3a18e5e111b5eaab095312d7fe84"],
    ["ethereum", "0x0ab87046fbb341d058f17cbc4c1133f25a20a52f"],
    ["ethereum", "0xfeef77d3f69374f66429c91d732a244f074bdf74"],
    ["ethereum", "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599"],
    ["ethereum", "0xdb25ca703181e7484a155dd612b06f57e12be5f0"],
    ["ethereum", "0xa258c4606ca8206d8aa700ce2143d7db854d168c"],
    ["ethereum", "0x5faf6a2d186448dfa667c51cb3d695c7a6e52d8e"],
    ["ethereum", "0xda816459f1ab5631232fe5e97a05bbbb94970c95"],
    ["ethereum", "0x8798249c2e607446efb7ad49ec89dd1865ff4272"],
    ["ethereum", "0xaa5a67c256e27a5d80712c51971408db3370927d"],
    ["ethereum", "0x27b7b1ad7288079a66d12350c828d3c00a6f07d7"],
    ["ethereum", "0x1635b506a88fbf428465ad65d00e8d6b6e5846c3"],
    ["ethereum", "0x865377367054516e17014ccded1e7d814edc9ce4"],
    ["ethereum", "0xa354f35829ae975e850e23e9615b11da1b3dc4de"],
    ["ethereum", "0x0000000000000000000000000000000000000000"],
    ["ethereum", "0x0bc529c00c6401aef6d220be8c6ea1667f6ad93e"],
    ["ethereum", "0x5ba61c0a8c4dcccc200cd0ccc40a5725a426d002"],
    ["ethereum", "0x7da96a3891add058ada2e826306d812c638d87a7"],
    ["ethereum", "0xd88dbba3f9c4391ee46f5ff548f289054db6e51c"],
    ["ethereum", "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"],
    ["ethereum", "0x41D5D79431A913C4aE7d69a668ecdfE5fF9DFB68"],
    ["ethereum", "0xAD038Eb671c44b853887A7E32528FaB35dC5D710"],
    ["ethereum", "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"],
    ["ethereum", "0xC285B7E09A4584D027E5BC36571785B515898246"],
    ["ethereum", "0xf939E0A03FB07F59A73314E73794Be0E57ac1b4E"],
    ["optimism", "0x7F5c764cBc14f9669B88837ca1490cCa17c31607"],
    ["optimism", "0x8aE125E8653821E851F12A49F7765db9a9ce7384"],
    ["optimism", "0x9560e827af36c94d2ac33a39bce1fe78631088db"],
    ["optimism", "0xdFA46478F9e5EA86d57387849598dbFB2e964b02"],
    ["optimism", "0x2E3D870790dC77A83DD1d18184Acc7439A53f475"],
    ["optimism", "0x73cb180bf0521828d8849bc8CF2B920918e23032"],
    ["optimism", "0xc5b001DC33727F8F26880B184090D3E252470D45"],
    ["bsc", "0x2952beb1326acCbB5243725bd4Da2fC937BCa087"],
    ["bsc", "0xFa4BA88Cf97e282c505BEa095297786c16070129"],
    ["bsc", "0x90C97F71E18723b0Cf0dfa30ee176Ab653E89F40"],
    ["bsc", "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c"],
    ["bsc", "0x2F29Bc0FFAF9bff337b31CBe6CB5Fb3bf12e5840"],
    ["arbitrum", "0x6A7661795C374c0bFC635934efAddFf3A7Ee23b6"],
    ["arbitrum", "0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8"],
    ["arbitrum", "0x3F56e0c36d275367b8C502090EDF38289b3dEa0d"],
    ["arbitrum", "0x17FC002b466eEc40DaE837Fc4bE5c67993ddBd6F"],
    ["arbitrum", "0xe80772Eaf6e2E18B651F160Bc9158b2A5caFCA65"],
    ["arbitrum", "0x8bc65Eed474D1A00555825c91FeAb6A8255C2107"],
    ["polygon", "0xa3Fa99A148fA48D14Ed51d610c367C61876997F1"],
    ["polygon", "0xbC2b48BC930Ddc4E5cFb2e87a45c379Aab3aac5C"],
    ["polygon", "0x80487b4f8f70e793A81a42367c225ee0B94315DF"],
    ["fantom", "0x3129662808bEC728a27Ab6a6b9AFd3cBacA8A43c"],
    ["fantom", "0xce1E3cc1950D2aAEb47dE04DE2dec2Dc86380E0A"],
    ["base", "0x4621b7A9c75199271F773Ebd9A499dbd165c3191"],
    ["base", "0xd9aAEc86B65D86f6A7B5B1b0c42FFA531710b6CA"],
    #["base", "0xbf1aeA8670D2528E08334083616dD9C5F3B087aE"],
]

data = {"coins": {}}

for i in range(0, len(token_address_list)):
    # For each token address, get the start date of the token /prices/first/coins
    url = f"https://coins.llama.fi/prices/first/{token_address_list[i][0]}:{token_address_list[i][1]}"
    data_part = requests.get(url).json()
    
    # add the start date to the token array as item 
    token_address_list[i].append(data_part["coins"][f"{token_address_list[i][0]}:{token_address_list[i][1]}"]["timestamp"])

    #round and change to match 23:59:59 of the same day
    token_address_list[i][2] = int(token_address_list[i][2])
    token_address_list[i][2] = token_address_list[i][2] - (token_address_list[i][2] % 86400) + 86340
    
    # add the number of days since the start date to the token_address_list item
    token_address_list[i].append((datetime.now() - datetime.fromtimestamp(token_address_list[i][2])).days)
    
    # then we can use the start date to get the data from the chart
    url = f"https://coins.llama.fi/chart/{token_address_list[i][0]}:{token_address_list[i][1]}?start={token_address_list[i][2]}&span={token_address_list[i][3]}&period=1d"
    data_chunk = requests.get(url).json()
    
    # add the data 
    data['coins'] = {**data['coins'], **data_chunk['coins']}

df = pd.DataFrame(data)

#turn the row name into a column
df.reset_index(level=0, inplace=True)

#rename first column to "chain:token_address"
df.rename(columns={"index": "chain:token_address"}, inplace=True)

#split the first column into two columns
df[['chain', 'token_address']] = df['chain:token_address'].str.split(':', expand=True)

#unnest "Coins" and add it to the dataframe
df = pd.concat([df.drop(['coins','chain:token_address'], axis=1), df['coins'].apply(pd.Series)], axis=1)

#unnest "prices" but recreate a row for each price
df = df.explode('prices')

# unnest "prices" and add it to the dataframe
df = pd.concat([df.drop(['prices'], axis=1), df['prices'].apply(pd.Series)], axis=1)

# extracty day from timestamp
df['day'] = pd.to_datetime(df['timestamp'], unit='s').dt.date

# format timestamp to datetime
df['timestamp'] = df['timestamp'].astype('datetime64[s]')

# Add chain_id column depending on the chain name
df['chain_id'] = df['chain'].map({'ethereum': 1, 
                                  'bsc': 56, 
                                  'polygon': 137, 
                                  'fantom': 250, 
                                  'optimism': 10, 
                                  'arbitrum': 42161, 
                                  'avax': 43114})

#if there is two rows with the same day and token_address, keep the most recent one
df = df.sort_values('timestamp').drop_duplicates(['day', 'token_address'], keep='last')

for token_address in token_address_list:
    ##for each token_address in token_address_list[1], get a range of the dates between min date and max date in the df
    # get the min date
    min_date = df[df['token_address'] == token_address[1]]['day'].min()
    # get the max date
    max_date = df[df['token_address'] == token_address[1]]['day'].max()
    # create a list of dates between min and max date
    date_list = []
    current_date = min_date
    
    while current_date <= max_date:
        date_list.append(current_date)
        current_date += timedelta(days=1)

    # for all the dates in the list, if there is no observation for the token_address, create one with the previous price and add it to the df
    for date in date_list:
        if not df[(df['token_address'] == token_address[1]) & (df['day'] == date)].empty:
            continue
        else:
            # get the previous day
            previous_date = date - timedelta(days=1)
            # get the previous price
            previous_price = df[(df['token_address'] == token_address[1]) & (df['day'] == previous_date)]['price'].values[0]
            previous_chain = df[(df['token_address'] == token_address[1]) & (df['day'] == previous_date)]['chain'].values[0]
            previous_chain_id = df[(df['token_address'] == token_address[1]) & (df['day'] == previous_date)]['chain_id'].values[0]
            previous_symbol = df[(df['token_address'] == token_address[1]) & (df['day'] == previous_date)]['symbol'].values[0]
            previous_confidence = df[(df['token_address'] == token_address[1]) & (df['day'] == previous_date)]['confidence'].values[0]
            previous_decimals = df[(df['token_address'] == token_address[1]) & (df['day'] == previous_date)]['decimals'].values[0]

            # create a new row with the previous price and the current date
            new_row = {
                'day': date,
                'timestamp': date,
                'chain': previous_chain,
                'chain_id': previous_chain_id,
                'token_address': token_address[1],
                'symbol': previous_symbol,
                'confidence': previous_confidence,
                'decimals': previous_decimals,
                'price': previous_price
            }
            # append the new row to the df
            df = df.append(new_row, ignore_index=True)

result = {
    "columns": [
        {"name": "day", "type": "integer", "friendly_name": "day"},
        {"name": "timestamp", "type": "integer", "friendly_name": "timestamp"},
        {"name": "chain", "type": "string", "friendly_name": "chain"},
        {"name": "chain_id", "type": "string", "friendly_name": "chain_id"},
        {"name": "token_address", "type": "string", "friendly_name": "token_address"},
        {"name": "symbol", "type": "string", "friendly_name": "symbol"},
        {"name": "confidence", "type": "float", "friendly_name": "confidence"},
        {"name": "decimals", "type": "integer", "friendly_name": "decimals"},
        {"name": "price", "type": "float", "friendly_name": "price"}
    ],
    "rows": df[['day','timestamp','chain', 'chain_id', 'token_address', 'symbol', 'confidence', 'decimals',  'price']].to_dict('records')
}
