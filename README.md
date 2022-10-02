# OneCall Crypto Trading package

onecall library is used to connect and trade with 
cryptocurrency exchanges and payment processing services 
worldwide. It provides quick access to market data for 
storage, analysis, visualization, indicator development, 
algorithmic trading, strategy backtesting, bot programming, 
and related software engineering.

It is intended to be used by coders, developers, 
technically-skilled traders, data-scientists and 
financial analysts for building trading algorithms.

## Available Crypto exchanges
1. Binance
2. Phemex
3. Kucoin
4. Bybit
5. FTX
6. FTX.us

## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install foobar.

```bash
pip install onecall
```

## Getting available Exchange list

```python
import onecall

exchanges = onecall.exchanges

print(exchanges)
```

## Usage

### connecting to exchange

```python
from onecall import Binance

# create mainnet client
client = Biance(key="<api_key>",
              secret="<secret_key>")
              
# create testnet client
client = Biance(key="<api_key>",
              secret="<secret_key>",
              debug=True)
              
# get all positions for a symbol              
positions = client.get_positions("<symbol>")

#cancel all orders
response = client.cancel_orders("<symbol>")

# get kline data.
kline = client.get_data("<symbol>", client.INTERVAL_30m, is_dataframe=True) #is_dataframe flag return data as pandas.Dataframe instead of json

# get orderbook
orderbook = client.get_orderbook("<symbol>", is_dataframe=True) #is_dataframe flag return data as pandas.Dataframe instead of json

#get account balance
balance = client.get_balance()

# place market order
response = client.market_order("<symbol>", client.BUY_SIDE, 10)

# place limit order
response = client.limit_order("<symbol>", client.BUY_SIDE, 10, .02000)

```

## Contributing
Pull requests are welcome. For major changes, please open an 
issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License
[MIT](https://choosealicense.com/licenses/mit/)
