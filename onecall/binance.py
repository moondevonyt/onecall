import datetime
import hmac
import hashlib
import logging
import pandas as pd
from urllib.parse import urlencode

from base import utils
from base.exchange import Exchange
from base import urls


class Binance(Exchange):
    """
    Binance API class
    :param key: api key
    :param secret: secret key
    Keyword Args:
        spot (boolean, optional): switch to sport market
        test (boolean, optional): switch to test env
        show_limit_usage (bool, optional): whether return limit usage(requests and/or orders). By default, it's False
    """
    def __init__(self, key=None, secret=None, test=False, **kwargs):
        self._path_config = {
            "get_positions": {"method": "GET", "path": "/fapi/v2/positionRisk", "rate_limit": 50},
            "cancel_orders": {"method": "DELETE", "path": "/fapi/v1/allOpenOrders", "rate_limit": 50},
            "get_data": {"method": "GET", "path": "/fapi/v1/klines", "rate_limit": 50},
            "get_orderbook": {"method": "GET", "path": "/fapi/v1/depth", "rate_limit": 50},
            "get_balance": {"method": "GET", "path": "/fapi/v2/balance", "rate_limit": 50},
            "market_order": {"method": "POST", "path": "/fapi/v1/order", "rate_limit": 50},
            "limit_order": {"method": "POST", "path": "/fapi/v1/order", "rate_limit": 50},
            "get_closed_orders": {"method": "GET", "path": "/fapi/v1/allOrders", "rate_limit": 50},
            "get_open_orders": {"method": "GET", "path": "/fapi/v1/openOrders", "rate_limit": 50}
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

        if not test:
            kwargs["base_url"] = urls.BINANCE_FUT_BASE_URL
        else:
            kwargs["base_url"] = urls.BINANCE_FUT_TEST_BASE_URL
        super().__init__(key, secret, **kwargs)
        return

    def get_positions(self, symbol: str):
        """
        API to get current positions
        :param symbol: future symbol
        :return:[
            {
                "entryPrice": "0.00000",
                "marginType": "isolated",
                "isAutoAddMargin": "false",
                "isolatedMargin": "0.00000000",
                "leverage": "10",
                "liquidationPrice": "0",
                "markPrice": "6679.50671178",
                "maxNotionalValue": "20000000",
                "positionAmt": "0.000",
                "notional": "0",,
                "isolatedWallet": "0",
                "symbol": "BTCUSDT",
                "unRealizedProfit": "0.00000000",
                "positionSide": "BOTH",
                "updateTime": 0
            }
        ]
        """
        params = {
            "symbol": symbol,
            "timestamp": utils.get_current_timestamp()
        }
        response = self._signed_request(self._path_config.get("get_positions").get("method"),
                                        self._path_config.get("get_positions").get("path"),
                                        params)
        return response

    def cancel_orders(self, symbol: str):
        """
        API to cancel all the active orders
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
        :param symbol: future symbol
        :param interval: time interval
        Keyword Args:
            start_date: start time of the data
            end_date: end time of the data
            limit: number of data limit
            s_dataframe: convert the data to pandas dataframe
        :return: list of list/ pandas dataframe
        """
        params = {
            "symbol": symbol,
            "interval": interval,
            "timestamp": utils.get_current_timestamp(),
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
                logging.error("failed to create dataframe")
        return response

    def get_orderbook(self, symbol: str, **kwargs):
        """
        Get orderbook
        :param symbol: future symbol
        Keyword Args:
            limit: result limit
            is_dataframe: convert the data to pandas dataframe
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
                df = pd.DataFrame(response["bids"], columns=columns)
                orderbook = df.append(pd.DataFrame(response["asks"], columns=columns), ignore_index=True)
                return orderbook
            except Exception as e:
                logging.error("failed to create dataframe")
        return response

    def get_balance(self):
        """
        API to get future account balance
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
        return list(filter(lambda order: order["status"] == "FILLED", response))

    def get_open_orders(self, symbol: str):
        """
        API to get all active orders
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
