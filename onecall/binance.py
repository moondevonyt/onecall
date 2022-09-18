import datetime
import hmac
import hashlib
import logging
import pandas as pd
from ratelimiter import RateLimiter

from base import utils
from base.exchange import Exchange
from base import urls


class Binance(Exchange):

    def __init__(self, key=None, secret=None, **kwargs):
        self._path_config = {
            "get_position": {"method": "GET", "path": "/fapi/v2/positionRisk", "rate_limit": 50},
            "cancel_order": {"method": "DELETE", "path": "/fapi/v1/batchOrders", "rate_limit": 50},
            "get_data": {"method": "GET", "path": "/fapi/v1/klines", "rate_limit": 50},
            "get_orderbook": {"method": "GET", "path": "/fapi/v1/depth", "rate_limit": 50},
            "get_balance": {"method": "GET", "path": "/fapi/v2/balance", "rate_limit": 50},
            "market_order": {"method": "POST", "path": "/fapi/v1/order", "rate_limit": 50},
            "limit_order": {"method": "POST", "path": "/fapi/v1/order", "rate_limit": 50},
            "get_closed_orders": {"method": "GET", "path": "/fapi/v1/allOrders", "rate_limit": 50},
            "get_open_orders": {"method": "GET", "path": "/fapi/v1/openOrders", "rate_limit": 50}
        }
        if "base_url" not in kwargs:
            kwargs["base_url"] = urls.BINANCE_FUT_BASE_URL

        super().__init__(key, secret, **kwargs)

    @RateLimiter(max_calls=50, period=1)
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
        response, error = self._signed_request(self._path_config.get("get_positions").get("method"),
                                               self._path_config.get("get_positions").get("path"),
                                               params)
        return response

    @RateLimiter(max_calls=50, period=1)
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

    @RateLimiter(max_calls=50, period=1)
    def get_data(self, symbol: str, interval: int, st_date: datetime.datetime, ed_date: datetime.datetime,
                 limit=500, is_dataframe=False):
        """
        API to get OHLCV data
        :param symbol: future symbol
        :param interval: time interval
        :param st_date: start time of the data
        :param ed_date: end time of the data
        :param limit: number of data limit
        :param is_dataframe: convert the data to pandas dataframe
        :return: list of list/ pandas dataframe
        """
        params = {
            "symbol": symbol,
            "interval": interval,
            "timestamp": utils.get_current_timestamp(),
            "limit": limit,
            "startTime": int(st_date.timestamp() * 1000),
            "endTime": int(ed_date.timestamp() * 1000)
        }
        header = {"X-MBX-APIKEY": self.key}
        response = self.send_request(self._path_config.get("get_data").get("method"),
                                     self._path_config.get("get_data").get("path"),
                                     header, params=params)
        if is_dataframe:
            try:
                columns = ['Open Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close time',
                           'Quote asset volume', 'Number of trades', 'Taker buy base asset volume',
                           'Taker buy quote asset volume']
                return pd.DataFrame(response, columns=columns)
            except :
                logging.error("failed to create dataframe")
        return response

    @RateLimiter(max_calls=50, period=1)
    def get_orderbook(self, symbol: str, limit=500, is_dataframe=False):
        """
        Get orderbook
        :param symbol: future symbol
        :param limit: result limit
        :param is_dataframe: convert the data to pandas dataframe
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
            "limit": limit
        }
        header = {"X-MBX-APIKEY": self.key}
        response = super().send_request(self._path_config.get("get_orderbook").get("method"),
                                        self._path_config.get("get_orderbook").get("path"),
                                        header, params=params)
        if is_dataframe:
            try:
                columns = ['price', 'QTY']
                df = pd.DataFrame(response["bids"], columns=columns)
                orderbook = df.append(pd.DataFrame(response["asks"], columns=columns), ignore_index=True)
                return orderbook
            except:
                logging.error("failed to create dataframe")
        return response

    @RateLimiter(max_calls=50, period=1)
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

    @RateLimiter(max_calls=50, period=1)
    def market_order(self, symbol: str, side: str, quantity: int):
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

    @RateLimiter(max_calls=50, period=1)
    def limit_order(self, symbol: str, side: str, quantity: int, price: float, time_in_force="GTC"):
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

    @RateLimiter(max_calls=50, period=1)
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

    @RateLimiter(max_calls=50, period=1)
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
        header, singed_params = self._get_request_credentials(params)
        response = self.send_request(method, url, header, singed_params)
        return response

    def _get_sign(self, data):
        m = hmac.new(self.secret.encode("utf-8"), str(data).encode("utf-8"), hashlib.sha256)
        return m.hexdigest()

    def _get_request_credentials(self, params):
        header = {"X-MBX-APIKEY": self.key}
        singed_params = {**params, "signature": self._get_sign(params)}
        return header, singed_params
