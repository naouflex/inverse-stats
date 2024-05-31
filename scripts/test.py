import requests

headers = {"Authorization": "Bearer XjIXK6kicLYBfUEnlDtGTwIXp5ZMm35X"}
params = {
            "src": "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
            "dst": "0x41D5D79431A913C4aE7d69a668ecdfE5fF9DFB68",
            "amount": "1000000000000000000",
            "from": "0xfda9365e2cdf21d72cb0dc4f5ff46f29e4ac59ce",
            "slippage": "1"
            }


response = requests.get("https://api.1inch.dev/swap/v5.2/1/swap", headers=headers, params=params)

print(response.json())