import hmac
import hashlib
import json
import logging
import pandas as pd
import time
from urllib.parse import urlencode

from base import utils
from base.exchange import Exchange
from base import urls


class FTX(Exchange):
    """
    FTX API Class
    """
    def __init__(self, key=None, secret=None, debug=False, **kwargs):
        self._path_config = {
            "get_positions": {"method": "GET", "path": "/positions", "rate_limit": 50},
            "cancel_orders": {"method": "DELETE", "path": "/orders", "rate_limit": 50},
            "get_data": {"method": "GET", "path": "/markets/{symbol}/candles", "rate_limit": 50},
            "get_orderbook": {"method": "GET", "path": "/markets/{symbol}/orderbook", "rate_limit": 50},
            "get_balance": {"method": "GET", "path": "/wallet/balances", "rate_limit": 50},
            "market_order": {"method": "POST", "path": "/orders", "rate_limit": 50},
            "limit_order": {"method": "POST", "path": "/orders", "rate_limit": 50},
            "get_closed_orders": {"method": "GET", "path": "/orders/history", "rate_limit": 50},
            "get_open_orders": {"method": "GET", "path": "/orders", "rate_limit": 50}
        }
        if not debug:
            kwargs["base_url"] = urls.FTX_FUT_BASE_URL
        else:
            kwargs["base_url"] = urls.FTX_FUT_TEST_BASE_URL
        super().__init__(key, secret, **kwargs)
        return

    def get_positions(self, symbol):
        """
        API to get current positions

        :param symbol: symbol
        :return: {
              "success": true,
              "result": [
                {
                  "cost": -31.7906,
                  "cumulativeBuySize": 1.2,
                  "cumulativeSellSize": 0.0,
                  "entryPrice": 138.22,
                  "estimatedLiquidationPrice": 152.1,
                  "future": "ETH-PERP",
                  "initialMarginRequirement": 0.1,
                  "longOrderSize": 1744.55,
                  "maintenanceMarginRequirement": 0.04,
                  "netSize": -0.23,
                  "openSize": 1744.32,
                  "realizedPnl": 3.39441714,
                  "recentAverageOpenPrice": 135.31,
                  "recentBreakEvenPrice": 135.31,
                  "recentPnl": 3.1134,
                  "shortOrderSize": 1732.09,
                  "side": "sell",
                  "size": 0.23,
                  "unrealizedPnl": 0,
                  "collateralUsed": 3.17906
                }
              ]
            }
        """
        response = self._signed_request(self._path_config.get("get_positions").get("method"),
                                        self._path_config.get("get_positions").get("path"))
        if response.get("result"):
            return list(filter(lambda pos: pos.get("future") == symbol, response.get("result")))
        return response

    def cancel_orders(self, symbol):
        """
        API to cancel all orders

        :param symbol: contract symbol
        :return: {
                  "success": true,
                  "result": "Orders queued for cancelation"
                }
        """
        payload = {
          "market": symbol,
        }
        response = self._signed_request(self._path_config.get("cancel_orders").get("method"),
                                        self._path_config.get("cancel_orders").get("path"),
                                        payload)
        return response

    def get_data(self, symbol, interval=15, **kwargs):
        """
        API to get data

        :param symbol: contract symbol
        :param interval: interval
        :keyword start_time: start time
        :keyword end_time: end time
        :keyword is_dataframe: convert to dataframe or not
        :return: {
              "success": true,
              "result": [
                {
                  "close": 11055.25,
                  "high": 11089.0,
                  "low": 11043.5,
                  "open": 11059.25,
                  "startTime": "2019-06-24T17:15:00+00:00",
                  "volume": 464193.95725
                }
              ]
            }
        """
        params = {
            "resolution": interval,
            **kwargs
        }
        response = self._signed_request(self._path_config.get("get_data").get("method"),
                                        self._path_config.get("get_data").get("path").format(symbol=symbol),
                                        params)
        if response.get("is_dataframe") and response.get("result"):
            try:
                return pd.DataFrame(response.get("result"))
            except Exception as e:
                logging.error("failed to create dataframe: ", e)
        return response

    def get_orderbook(self, symbol, limit=100, is_dataframe=False):
        """
        API to get orderbook

        :param symbol: contract symbol
        :param limit: data limit
        :param is_dataframe: convert the data to dataframe or not
        :return: {
              "success": true,
              "result": {
                "asks": [
                  [
                    4114.25,
                    6.263
                  ]
                ],
                "bids": [
                  [
                    4112.25,
                    49.29
                  ]
                ]
              }
            }
        """
        params = {
            "depth": limit
        }
        response = self._signed_request(self._path_config.get("get_orderbook").get("method"),
                                        self._path_config.get("get_orderbook").get("path").format(symbol=symbol),
                                        params)
        if response.get("is_dataframe") and response.get("result"):
            try:
                columns = ['price', 'QTY']
                df = pd.DataFrame(response["result"]["bids"], columns=columns)
                orderbook = df.append(pd.DataFrame(response["result"]["asks"], columns=columns), ignore_index=True)
                return orderbook
            except Exception as e:
                logging.error("failed to create dataframe: ", e)
        return response

    def get_balance(self):
        """
        API to get the balance

        :return: {
              "success": true,
              "result": {
                "main": [
                  {
                    "coin": "USDTBEAR",
                    "free": 2320.2,
                    "spotBorrow": 0.0,
                    "total": 2340.2,
                    "usdValue": 2340.2,
                    "availableWithoutBorrow": 2320.2
                  },
                  {
                    "coin": "BTC",
                    "free": 2.0,
                    "spotBorrow": 0.0,
                    "total": 3.2,
                    "usdValue": 23456.7,
                    "availableWithoutBorrow": 2.0
                  }
                ],
            }
        }
        """
        response = self._signed_request(self._path_config.get("get_balance").get("method"),
                                        self._path_config.get("get_balance").get("path"))
        return response

    def market_order(self, symbol, side, quantity, **kwargs):
        """
        API to place market order

        :param symbol: contract symbol
        :param side: buy/sell
        :param quantity: order quantity
        :return: {
              "success": true,
              "result": {
                "createdAt": "2019-03-05T09:56:55.728933+00:00",
                "filledSize": 0,
                "future": "XRP-PERP",
                "id": 9596912,
                "market": "XRP-PERP",
                "price": 0.306525,
                "remainingSize": 31431,
                "side": "sell",
                "size": 31431,
                "status": "open",
                "type": "market",
                "reduceOnly": false,
                "ioc": false,
                "postOnly": false,
                "clientId": null,
              }
            }
        """
        payload = {
            "market": symbol,
            "side": side,
            "type": "market",
            "size": quantity,
            **kwargs
        }
        response = self._signed_request(self._path_config.get("market_order").get("method"),
                                        self._path_config.get("market_order").get("path"),
                                        data=payload)
        return response

    def limit_order(self, symbol, side, quantity, price, **kwargs):
        """
        API to place limit order

        :param symbol: contract symbol
        :param side: buy/sell
        :param quantity: order quantity
        :param price: order price
        :keyword post_only: for post only request
        :return: {
              "success": true,
              "result": {
                "createdAt": "2019-03-05T09:56:55.728933+00:00",
                "filledSize": 0,
                "future": "XRP-PERP",
                "id": 9596912,
                "market": "XRP-PERP",
                "price": 0.306525,
                "remainingSize": 31431,
                "side": "sell",
                "size": 31431,
                "status": "open",
                "type": "limit",
                "reduceOnly": false,
                "ioc": false,
                "postOnly": false,
                "clientId": null,
              }
            }
        """
        payload = {
            "market": symbol,
            "side": side,
            "price": price,
            "type": "limit",
            "size": quantity,
            **kwargs
        }
        response = self._signed_request(self._path_config.get("limit_order").get("method"),
                                        self._path_config.get("limit_order").get("path"),
                                        data=payload)
        return response

    def get_closed_orders(self):
        """
        API to get all closed orders

        :return: {
              "success": true,
              "result": [
                {
                  "avgFillPrice": 10135.25,
                  "clientId": null,
                  "createdAt": "2019-06-27T15:24:03.101197+00:00",
                  "filledSize": 0.001,
                  "future": "BTC-PERP",
                  "id": 257132591,
                  "ioc": false,
                  "market": "BTC-PERP",
                  "postOnly": false,
                  "price": 10135.25,
                  "reduceOnly": false,
                  "remainingSize": 0.0,
                  "side": "buy",
                  "size": 0.001,
                  "status": "closed",
                  "type": "limit"
                },
              ],
              "hasMoreData": false,
            }
        """
        response = self._signed_request(self._path_config.get("limit_order").get("method"),
                                        self._path_config.get("limit_order").get("path"))
        if response.get("result"):
            return list(filter(lambda order: order.get("status", "") == "closed", response.get("result")))
        return response

    def get_open_orders(self):
        """
        API to get all open orders

        :return: {
              "success": true,
              "result": [
                {
                  "createdAt": "2019-03-05T09:56:55.728933+00:00",
                  "filledSize": 10,
                  "future": "XRP-PERP",
                  "id": 9596912,
                  "market": "XRP-PERP",
                  "price": 0.306525,
                  "avgFillPrice": 0.306526,
                  "remainingSize": 31421,
                  "side": "sell",
                  "size": 31431,
                  "status": "open",
                  "type": "limit",
                  "reduceOnly": false,
                  "ioc": false,
                  "postOnly": false,
                  "clientId": null
                }
              ]
            }
        """
        response = self._signed_request(self._path_config.get("get_open_orders").get("method"),
                                        self._path_config.get("get_open_orders").get("path"))
        return response

    def _signed_request(self, method, url, params=None, data=None):
        ts = str(utils.get_current_timestamp())
        params = urlencode(params) if params else None
        data = json.dump(data) if data else None
        header = self._get_request_credentials(ts, method, url, params, data)
        response = self.send_request(method, url, header, params, data)
        return response

    def _get_sign(self, data):
        m = hmac.new(self.secret.encode(), data.encode(), hashlib.sha256)
        return m.hexdigest()

    def _get_request_credentials(self, ts, method, path, params='', data=None):
        sign_string = f'{ts}{method}{path}?{params}'
        if data:
            sign_string += data
        sing = self._get_sign(sign_string)
        header = {
            "FTX-KEY": self.key,
            "FTX-SIGN": sing,
            "FTX-TS": ts
        }
        return header
