# inverse-stats

## Description

Python library used to simplify blockchain data analysis and publish periodic financial reporting.

The functions are used to populate a postgresql DB in order to make the processing of complex analysis time-savy.

### tools
- chainkit.py : a collectio of home made tools to deal with blocks and timeseries data
- database.py : a collection of python tools to quickly create and update production database

### blocks_daily
- script to constitute daily snapshot of the closest block to midnight

### dola_lp
- script to aggregate data for dola liquidity providers

### dola_supply
- script to aggregate data for dola supply from Fed Contracts

### defillama_prices
- script to get and update defi llama prices for a better availability/performance