import datetime
import hmac
import hashlib
import logging
import pandas as pd
from urllib.parse import urlencode

from onecall.base import utils
from onecall.base.exchange import Exchange
from onecall.base import urls


class BinanceSpot(Exchange):
    """
    Binance API class
    https://binance-docs.github.io/apidocs/spot/en/#general-api-information
    """
    def __init__(self, key=None, secret=None, debug=False, **kwargs):
        """
        Binance API

        :param key: api key
        :param secret: secret key
        :param debug: flag to switch to testnet
        """
        self._path_config = {
            "cancel_orders": {"method": "DELETE", "path": "/api/v3/openOrders", "rate_limit": 50},
            "get_data": {"method": "GET", "path": "/api/v3/klines", "rate_limit": 50},
            "get_orderbook": {"method": "GET", "path": "/api/v3/depth", "rate_limit": 50},
            "get_balance": {"method": "GET", "path": "/api/v3/account", "rate_limit": 50},
            "market_order": {"method": "POST", "path": "/api/v3/order", "rate_limit": 50},
            "limit_order": {"method": "POST", "path": "/api/v3/order", "rate_limit": 50},
            "get_closed_orders": {"method": "GET", "path": "/api/v3/allOrders", "rate_limit": 50},
            "get_open_orders": {"method": "GET", "path": "/api/v3/openOrders", "rate_limit": 50}
        }
        self._LIMIT = 500

        # Constants for Order side
        self.BUY_SIDE = 'BUY'
        self.SELL_SIDE = 'SELL'

        # binance interval
        self.INTERVAL_1m = '1m'
        self.INTERVAL_3m = '3m'
        self.INTERVAL_5m = '5m'
        self.INTERVAL_15m = '15m'
        self.INTERVAL_30m = '30m'
        self.INTERVAL_1H = '1h'
        self.INTERVAL_2H = '2h'
        self.INTERVAL_4H = '4h'
        self.INTERVAL_6H = '6h'
        self.INTERVAL_8H = '8h'
        self.INTERVAL_12H = '12h'
        self.INTERVAL_1D = '1d'
        self.INTERVAL_3D = '3d'
        self.INTERVAL_1W = '1w'
        self.INTERVAL_1M = '1M'

        if not debug:
            kwargs["base_url"] = urls.BINANCE_SPOT_BASE_URL
        else:
            kwargs["base_url"] = urls.BINANCE_SPOT_TEST_BASE_URL
        super().__init__(key, secret, **kwargs)
        return

    def cancel_orders(self, symbol: str):
        """
        API to cancel all the active orders
        https://binance-docs.github.io/apidocs/spot/en/#cancel-all-open-orders-on-a-symbol-trade

        :param symbol: future symbol
        :return: [
            {
                "clientOrderId": "myOrder1",
                "cumQty": "0",
                "cumQuote": "0",
                "executedQty": "0",
                "orderId": 283194212,
                "origQty": "11",
                "origType": "TRAILING_STOP_MARKET",
                "price": "0",
                "reduceOnly": false,
                "side": "BUY",
                "positionSide": "SHORT",
                "status": "CANCELED",
                "stopPrice": "9300",                // please ignore when order type is TRAILING_STOP_MARKET
                "closePosition": false,   // if Close-All
                "symbol": "BTCUSDT",
                "timeInForce": "GTC",
                "type": "TRAILING_STOP_MARKET",
                "activatePrice": "9020",            // activation price, only return with TRAILING_STOP_MARKET order
                "priceRate": "0.3",                 // callback rate, only return with TRAILING_STOP_MARKET order
                "updateTime": 1571110484038,
                "workingType": "CONTRACT_PRICE",
                "priceProtect": false            // if conditional order trigger is protected
            },
            {
                "code": -2011,
                "msg": "Unknown order sent."
            }
        ]
        """
        params = {
            "symbol": symbol,
            "timestamp": utils.get_current_timestamp()
        }
        response = self._signed_request(self._path_config.get("cancel_orders").get("method"),
                                        self._path_config.get("cancel_orders").get("path"),
                                        params)
        return response

    def get_data(self, symbol: str, interval: int, **kwargs):
        """
        API to get OHLCV data
        https://binance-docs.github.io/apidocs/spot/en/#uiklines

        :param symbol: future symbol
        :param interval: time interval
        :keyword start_date: start time of the data
        :keyword end_date: end time of the data
        :keyword limit: number of data limit
        :keyword is_dataframe: convert the data to pandas dataframe
        :return: list of list/ pandas dataframe
        """
        params = {
            "symbol": symbol,
            "interval": interval,
            "limit": kwargs.get("limit") if kwargs.get("limit", None) else self._LIMIT,
        }
        if kwargs.get("start_date"):
            params["startTime"] = int(kwargs["start_date"] * 1000)
        if kwargs.get("end_date"):
            params["endTime"] = int(kwargs["end_date"] * 1000)

        headers = {"X-MBX-APIKEY": self.key}
        response = self.send_request(self._path_config.get("get_data").get("method"),
                                     self._path_config.get("get_data").get("path"),
                                     headers, params)
        if kwargs.get("is_dataframe"):
            try:
                columns = ['Open Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close time',
                           'Quote asset volume', 'Number of trades', 'Taker buy base asset volume',
                           'Taker buy quote asset volume', 'ignore']
                return pd.DataFrame(response, columns=columns)
            except Exception as e:
                logging.error("failed to create dataframe: ", e)
        return response

    def get_orderbook(self, symbol: str, **kwargs):
        """
        Get orderbook
        https://binance-docs.github.io/apidocs/spot/en/#recent-trades-list

        :param symbol: future symbol
        :keyword limit: result limit
        :keyword is_dataframe: convert the data to pandas dataframe
        :return: {
              "lastUpdateId": 1027024,
              "E": 1589436922972,   // Message output time
              "T": 1589436922959,   // Transaction time
              "bids": [
                [
                  "4.00000000",     // PRICE
                  "431.00000000"    // QTY
                ]
              ],
              "asks": [
                [
                  "4.00000200",
                  "12.00000000"
                ]
              ]
            }
        """
        params = {
            "symbol": symbol,
            "limit": kwargs.get("limit") if kwargs.get("limit") else self._LIMIT
        }
        header = {"X-MBX-APIKEY": self.key}
        response = super().send_request(self._path_config.get("get_orderbook").get("method"),
                                        self._path_config.get("get_orderbook").get("path"),
                                        header, params=params)
        if kwargs.get("is_dataframe"):
            try:
                columns = ['price', 'QTY']
                bid = pd.DataFrame(response["bids"], columns=columns)
                bid["type"] = ["bid" for i in range(0, bid.shape[0])]
                ask = pd.DataFrame(response["asks"], columns=columns)
                ask["type"] = ["ask" for i in range(0, ask.shape[0])]
                df = pd.concat([bid, ask], ignore_index=True)
                return df
            except Exception as e:
                logging.error("failed to create dataframe: ", e)
        return response

    def get_balance(self):
        """
        API to get future account balance
        https://binance-docs.github.io/apidocs/spot/en/#account-trade-list-user_data

        :return: [
                    {
                        "accountAlias": "SgsR",    // unique account code
                        "asset": "USDT",    // asset name
                        "balance": "122607.35137903", // wallet balance
                        "crossWalletBalance": "23.72469206", // crossed wallet balance
                        "crossUnPnl": "0.00000000"  // unrealized profit of crossed positions
                        "availableBalance": "23.72469206",       // available balance
                        "maxWithdrawAmount": "23.72469206",     // maximum amount for transfer out
                        "marginAvailable": true,    // whether the asset can be used as margin in Multi-Assets mode
                        "updateTime": 1617939110373
                    }
                ]
        """
        params = {
            "timestamp": utils.get_current_timestamp()
        }
        response = self._signed_request(self._path_config.get("get_balance").get("method"),
                                        self._path_config.get("get_balance").get("path"),
                                        params)
        return response

    def market_order(self, symbol: str, side: str, quantity: float):
        """
        API to place market order
        https://binance-docs.github.io/apidocs/spot/en/#new-order-trade

        :param symbol: future symbol
        :param side: buy/sell
        :param quantity:
        :return: {
            "clientOrderId": "testOrder",
            "cumQty": "0",
            "cumQuote": "0",
            "executedQty": "0",
            "orderId": 22542179,
            "avgPrice": "0.00000",
            "origQty": "10",
            "price": "0",
            "reduceOnly": false,
            "side": "BUY",
            "positionSide": "SHORT",
            "status": "NEW",
            "stopPrice": "9300",        // please ignore when order type is TRAILING_STOP_MARKET
            "closePosition": false,   // if Close-All
            "symbol": "BTCUSDT",
            "timeInForce": "GTC",
            "type": "TRAILING_STOP_MARKET",
            "origType": "TRAILING_STOP_MARKET",
            "activatePrice": "9020",    // activation price, only return with TRAILING_STOP_MARKET order
            "priceRate": "0.3",         // callback rate, only return with TRAILING_STOP_MARKET order
            "updateTime": 1566818724722,
            "workingType": "CONTRACT_PRICE",
            "priceProtect": false            // if conditional order trigger is protected
        }
        """
        params = {
            "symbol": symbol,
            "side": side.upper(),
            "type": "MARKET",
            "quantity": quantity,
            "timestamp": utils.get_current_timestamp()
        }
        response = self._signed_request(self._path_config.get("market_order").get("method"),
                                        self._path_config.get("market_order").get("path"),
                                        params)
        return response

    def limit_order(self, symbol: str, side: str, quantity: float, price: float, time_in_force="GTC"):
        """
        API to place limit order
        https://binance-docs.github.io/apidocs/spot/en/#new-order-trade

        :param symbol: future symbol
        :param side: buy/sell
        :param quantity: trade quantity
        :param price: trading price
        :param time_in_force: for postOnly request
        :return: {
            "clientOrderId": "testOrder",
            "cumQty": "0",
            "cumQuote": "0",
            "executedQty": "0",
            "orderId": 22542179,
            "avgPrice": "0.00000",
            "origQty": "10",
            "price": "0",
            "reduceOnly": false,
            "side": "BUY",
            "positionSide": "SHORT",
            "status": "NEW",
            "stopPrice": "9300",        // please ignore when order type is TRAILING_STOP_MARKET
            "closePosition": false,   // if Close-All
            "symbol": "BTCUSDT",
            "timeInForce": "GTC",
            "type": "TRAILING_STOP_MARKET",
            "origType": "TRAILING_STOP_MARKET",
            "activatePrice": "9020",    // activation price, only return with TRAILING_STOP_MARKET order
            "priceRate": "0.3",         // callback rate, only return with TRAILING_STOP_MARKET order
            "updateTime": 1566818724722,
            "workingType": "CONTRACT_PRICE",
            "priceProtect": false            // if conditional order trigger is protected
        }
        """
        params = {
            "symbol": symbol,
            "side": side.upper(),
            "type": "LIMIT",
            "quantity": quantity,
            "price": price,
            "timeInForce": time_in_force,
            "timestamp": utils.get_current_timestamp()
        }
        response = self._signed_request(self._path_config.get("limit_order").get("method"),
                                        self._path_config.get("limit_order").get("path"),
                                        params)
        return response

    def get_closed_orders(self, symbol: str):
        """
        API to get all the closed orders
        https://binance-docs.github.io/apidocs/spot/en/#new-oco-trade

        :param symbol: future symbol
        :return: [
              {
                "avgPrice": "0.00000",
                "clientOrderId": "abc",
                "cumQuote": "0",
                "executedQty": "0",
                "orderId": 1917641,
                "origQty": "0.40",
                "origType": "TRAILING_STOP_MARKET",
                "price": "0",
                "reduceOnly": false,
                "side": "BUY",
                "positionSide": "SHORT",
                "status": "NEW",
                "stopPrice": "9300",                // please ignore when order type is TRAILING_STOP_MARKET
                "closePosition": false,   // if Close-All
                "symbol": "BTCUSDT",
                "time": 1579276756075,              // order time
                "timeInForce": "GTC",
                "type": "TRAILING_STOP_MARKET",
                "activatePrice": "9020",            // activation price, only return with TRAILING_STOP_MARKET order
                "priceRate": "0.3",                 // callback rate, only return with TRAILING_STOP_MARKET order
                "updateTime": 1579276756075,        // update time
                "workingType": "CONTRACT_PRICE",
                "priceProtect": false            // if conditional order trigger is protected
              }
            ]
        """
        params = {
            "symbol": symbol,
            "timestamp": utils.get_current_timestamp()
        }
        response = self._signed_request(self._path_config.get("get_closed_orders").get("method"),
                                        self._path_config.get("get_closed_orders").get("path"),
                                        params)
        if type(response) == list:
            return list(filter(lambda order: order["status"] == "FILLED", response))
        return response

    def get_open_orders(self, symbol: str):
        """
        API to get all active orders
        https://binance-docs.github.io/apidocs/spot/en/#all-orders-user_data

        :param symbol: symbol
        :return: [
              {
                "avgPrice": "0.00000",
                "clientOrderId": "abc",
                "cumQuote": "0",
                "executedQty": "0",
                "orderId": 1917641,
                "origQty": "0.40",
                "origType": "TRAILING_STOP_MARKET",
                "price": "0",
                "reduceOnly": false,
                "side": "BUY",
                "positionSide": "SHORT",
                "status": "NEW",
                "stopPrice": "9300",                // please ignore when order type is TRAILING_STOP_MARKET
                "closePosition": false,   // if Close-All
                "symbol": "BTCUSDT",
                "time": 1579276756075,              // order time
                "timeInForce": "GTC",
                "type": "TRAILING_STOP_MARKET",
                "activatePrice": "9020",            // activation price, only return with TRAILING_STOP_MARKET order
                "priceRate": "0.3",                 // callback rate, only return with TRAILING_STOP_MARKET order
                "updateTime": 1579276756075,        // update time
                "workingType": "CONTRACT_PRICE",
                "priceProtect": false            // if conditional order trigger is protected
              }
            ]
        """
        params = {
            "symbol": symbol,
            "timestamp": utils.get_current_timestamp()
        }
        response = self._signed_request(self._path_config.get("get_open_orders").get("method"),
                                        self._path_config.get("get_open_orders").get("path"),
                                        params)
        return response

    def _signed_request(self, method, url, params):
        headers = {"X-MBX-APIKEY": self.key}
        signature = self._get_request_credentials(params)
        params["signature"] = signature
        response = self.send_request(method, url, headers, urlencode(params))
        return response

    def _get_sign(self, data):
        m = hmac.new(self.secret.encode("utf-8"), str(data).encode("utf-8"), hashlib.sha256)
        return m.hexdigest()

    def _get_request_credentials(self, params):
        sign = self._get_sign(urlencode(params))
        return sign
