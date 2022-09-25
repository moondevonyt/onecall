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

## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install foobar.

```bash
pip install onecall
```

## Usage

```python
from onecall import Binance

# create client
client = Biance(key="5hQJkQUprFQwNGIz3l",
              secret="jnPXDWJ7OZmCLBwTSc0IHiGTIXmpEBlGM9pY")

# returns open order list
open_orders = client.get_open_orders()

# print open orders
print(open_orders)
```

## Contributing
Pull requests are welcome. For major changes, please open an 
issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License
[MIT](https://choosealicense.com/licenses/mit/)