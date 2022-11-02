import hmac
import hashlib
import json
import logging
import uuid

import pandas as pd
from urllib.parse import urlencode

from onecall.base import utils
from onecall.base.exchange import Exchange
from onecall.base import urls


class Phemex(Exchange):
    """
    Phemex API class
    https://github.com/phemex/phemex-api-docs/blob/master/Public-Contract-API-en.md
    """
    # Interval constants
    MINUTE_1 = 60
    MINUTE_5 = 300
    MINUTE_15 = 900
    MINUTE_30 = 1800
    HOUR_1 = 3600
    HOUR_4 = 14400
    DAY_1 = 86400
    WEEK_1 = 604800
    MONTH_1 = 2592000
    SEASON_1 = 7776000
    YEAR_1 = 31104000

    # Limit constants
    LIMIT_5 = 5
    LIMIT_10 = 10
    LIMIT_50 = 50
    LIMIT_100 = 100
    LIMIT_500 = 500
    LIMIT_1000 = 1000

    _LIMIT = 500

    # Constants for Order side
    BUY_SIDE = 'Buy'
    SELL_SIDE = 'Sell'

    def __init__(self, key=None, secret=None, debug=False, **kwargs):
        """
        Phemex API class
        https://github.com/phemex/phemex-api-docs/blob/master/Public-Contract-API-en.md

        :param key: key
        :param secret: secret key
        :param debug: connect to testnet
        """
        self._path_config = {
            "get_positions": {"method": "GET", "path": "/accounts/accountPositions", "rate_limit": 50},
            "cancel_orders": {"method": "DELETE", "path": "/orders/all", "rate_limit": 50},
            "get_data": {"method": "GET", "path": "/exchange/public/md/v2/kline", "rate_limit": 50},
            "get_orderbook": {"method": "GET", "path": "/md/orderbook", "rate_limit": 50},
            "get_balance": {"method": "GET", "path": "/accounts/accountPositions", "rate_limit": 50},
            "market_order": {"method": "POST", "path": "/orders", "rate_limit": 50},
            "limit_order": {"method": "POST", "path": "/orders", "rate_limit": 50},
            "get_closed_orders": {"method": "GET", "path": "/exchange/order/list", "rate_limit": 50},
            "get_open_orders": {"method": "GET", "path": "/orders/activeList", "rate_limit": 50}
        }

        if not debug:
            kwargs["base_url"] = urls.PHEMEX_FUT_BASE_URL
        else:
            kwargs["base_url"] = urls.PHEMEX_FUT_TEST_BASE_URL
        super().__init__(key, secret, **kwargs)
        return

    def get_positions(self, currency="USD"):
        """
        API to get current positions
        https://github.com/phemex/phemex-api-docs/blob/master/Public-Contract-API-en.md#querytradeaccount

        :param currency: symbol
        :return: {
                "code": 0,
                    "msg": "",
                    "data": {
                        "positions": [
                            {
                                "accountID": 0,
                                "symbol": "BTCUSD",
                                "currency": "BTC",
                                "side": "None",
                                "positionStatus": "Normal",
                                "crossMargin": false,
                                "leverageEr": 0,
                                "leverage": 0,
                                "initMarginReqEr": 0,
                                "initMarginReq": 0.01,
                                "maintMarginReqEr": 500000,
                                "maintMarginReq": 0.005,
                                "riskLimitEv": 10000000000,
                                "riskLimit": 100,
                                "size": 0,
                                "value": 0,
                                "valueEv": 0,
                                ...
                            }
                        ]
                    }
            }
        """
        params = {
            "currency": currency
        }
        response = self._signed_request(self._path_config.get("get_positions").get("method"),
                                        self._path_config.get("get_positions").get("path"),
                                        params)
        if response.get("data", None):
            return response.get("data", {}).get("positions")
        else:
            self._logger.error(response)
            return response

    def cancel_orders(self, symbol: str):
        """
        API to cancel all orders
        https://github.com/phemex/phemex-api-docs/blob/master/Public-Contract-API-en.md#cancelall

        :param symbol: future symbol
        :return: data part of response is subject to change
        """
        params = {
            "symbol": symbol,
            "untriggered": True,
        }
        response = self._signed_request(self._path_config.get("cancel_orders").get("method"),
                                        self._path_config.get("cancel_orders").get("path"),
                                        params)
        return response

    def get_data(self, symbol: str, interval: int, **kwargs):
        """
        API to get OHLCV data
        https://github.com/phemex/phemex-api-docs/blob/master/Public-Contract-API-en.md#querykline

        :param symbol: future symbol
        :param interval: interval
        :keyword limit: data limit
        :keyword is_dataframe: convert the data to pandas dataframe
        :return: {
            "code": 0,
            "msg": "OK",
            "data": {
                    "total": -1,
                    "rows": [
                        [<timestamp>, <interval>, <last_close>, <open>, <high>, <low>, <close>, <volume>, <turnover>],
                        ]
                }
            }
        """
        params = {
            "symbol": symbol,
            "resolution": interval,
            **kwargs
        }
        response = self._signed_request(self._path_config.get("get_data").get("method"),
                                        self._path_config.get("get_data").get("path"),
                                        params)
        if kwargs.get("is_dataframe", None):
            try:
                columns = ["timestamp", "interval", "last_close", "open", "high", "low", "close", "volume", "turnover"]
                return pd.DataFrame(response["data"]["rows"], columns=columns)
            except Exception as e:
                self._logger.error(e)
        return response

    def get_orderbook(self, symbol: str, is_dataframe=False):
        """
        API to get orderbook
        https://github.com/phemex/phemex-api-docs/blob/master/Public-Contract-API-en.md#queryorderbook

        :param symbol: future_symbol
        :param is_dataframe: whether to return row json/dataframe
        :return: {
              "error": null,
              "id": 0,
              "result": {
                "book": {
                  "asks": [[<priceEp>, size>],],
                  "bids": [[<priceEp>, <size>],],
                },
                "depth": 30,
                "sequence": <sequence>,
                "timestamp": <timestamp>,
                "symbol": "<symbol>",
                "type": "snapshot"
              }
            }
        """
        params = {
            "symbol": symbol
        }
        response = self._signed_request(self._path_config.get("get_orderbook").get("method"),
                                        self._path_config.get("get_orderbook").get("path"),
                                        params)
        if is_dataframe and response.get("result"):
            try:
                columns = ['price', 'QTY']
                bid = pd.DataFrame(response["result"]["book"]["bids"], columns=columns)
                bid["type"] = ["bid" for i in range(0, bid.shape[0])]
                ask = pd.DataFrame(response["result"]["book"]["asks"], columns=columns)
                ask["type"] = ["ask" for i in range(0, ask.shape[0])]
                df = pd.concat([bid, ask], ignore_index=True)
                return df
            except Exception as e:
                logging.error(e)
        return response

    def get_balance(self, currency="USD"):
        """
        API to get account balance
        https://github.com/phemex/phemex-api-docs/blob/master/Public-Contract-API-en.md#querytradeaccount

        :param currency: currency. USD,BTC
        :return: {
            "code": 0,
                "msg": "",
                "data": {
                    "account": {
                        "accountId": 0,
                        "currency": "BTC",
                        "accountBalanceEv": 0,
                        "totalUsedBalanceEv": 0
                    },
                }
            }
        """
        params = {
            "currency": currency
        }
        response = self._signed_request(self._path_config.get("get_balance").get("method"),
                                        self._path_config.get("get_balance").get("path"),
                                        params)
        if response.get("data"):
            balance = response.get("data", {}).get("account")
            if currency == 'USD':
                balance["balance"] = balance.get("accountBalanceEv", 0) / 10000
            elif currency.upper() == 'BTC':
                balance["balance"] = balance.get("accountBalanceEv", 0) / 100000000
            return balance
        return response

    def market_order(self, symbol: str, side: str, order_qty: float, **kwargs):
        """
        API to place market order
        https://github.com/phemex/phemex-api-docs/blob/master/Public-Contract-API-en.md#placeorder

        :param symbol: coin symbol
        :param side: BUY/SELL
        :param order_qty: order quantity
        :keyword timeInForce: Time in force. default to GoodTillCancel
        :keyword reduceOnly: whether reduce position side only
        :return: {
                "code": 0,
                    "msg": "",
                    "data": {
                        "bizError": 0,
                        "orderID": "ab90a08c-b728-4b6b-97c4-36fa497335bf",
                        "clOrdID": "137e1928-5d25-fecd-dbd1-705ded659a4f",
                        "symbol": "BTCUSD",
                        "side": "Sell",
                        "actionTimeNs": 1580547265848034600,
                        "transactTimeNs": 0,
                        "orderType": null,
                        "priceEp": 98970000,
                        "price": 9897,
                        "orderQty": 1,
                        "displayQty": 1,
                        "timeInForce": null,
                        "reduceOnly": false,
                        "stopPxEp": 0,
                        "closedPnlEv": 0,
                        "closedPnl": 0,
                        "closedSize": 0,
                        "cumQty": 0,
                        "cumValueEv": 0,
                        "cumValue": 0,
                        "leavesQty": 1,
                        "leavesValueEv": 10104,
                        "leavesValue": 0.00010104,
                        "stopPx": 0,
                        "stopDirection": "UNSPECIFIED",
                        "ordStatus": "Created"
                    }
            }
        """
        payload = {
            "clOrdID": str(uuid.uuid1()),
            "symbol": symbol,
            "side": side,
            "ordType": "Market",
            "orderQty": order_qty,
            **kwargs
        }
        response = self._signed_request(self._path_config.get("market_order").get("method"),
                                        self._path_config.get("market_order").get("path"),
                                        data=payload)
        return response

    def limit_order(self, symbol: str, side: str, order_qty: int, price: float, **kwargs):
        """
        API to place limit order
        https://github.com/phemex/phemex-api-docs/blob/master/Public-Contract-API-en.md#placeorder

        :param symbol: currency symbol
        :param side: BUY/SELL
        :param order_qty: order quantity
        :param price: order price
        :keyword timeInForce: Time in force. default to GoodTillCancel
        :keyword reduceOnly: whether reduce position side only
        :return: {
                "code": 0,
                    "msg": "",
                    "data": {
                        "bizError": 0,
                        "orderID": "ab90a08c-b728-4b6b-97c4-36fa497335bf",
                        "clOrdID": "137e1928-5d25-fecd-dbd1-705ded659a4f",
                        "symbol": "BTCUSD",
                        "side": "Sell",
                        "actionTimeNs": 1580547265848034600,
                        "transactTimeNs": 0,
                        "orderType": null,
                        "priceEp": 98970000,
                        "price": 9897,
                        "orderQty": 1,
                        "displayQty": 1,
                        "timeInForce": null,
                        "reduceOnly": false,
                        "stopPxEp": 0,
                        "closedPnlEv": 0,
                        "closedPnl": 0,
                        "closedSize": 0,
                        "cumQty": 0,
                        "cumValueEv": 0,
                        "cumValue": 0,
                        "leavesQty": 1,
                        "leavesValueEv": 10104,
                        "leavesValue": 0.00010104,
                        "stopPx": 0,
                        "stopDirection": "UNSPECIFIED",
                        "ordStatus": "Created"
                    }
            }
        """
        payload = {
            "clOrdID": str(uuid.uuid1()),
            "symbol": symbol,
            "side": side,
            "ordType": "Limit",
            "orderQty": order_qty,
            "priceEp": price,
            **kwargs
        }
        response = self._signed_request(self._path_config.get("limit_order").get("method"),
                                        self._path_config.get("limit_order").get("path"),
                                        data=payload)
        return response

    def get_closed_orders(self, symbol):
        """
        API to get closed order
        https://github.com/phemex/phemex-api-docs/blob/master/Public-Contract-API-en.md#queryorder

        :param symbol: currency symbol
        :return: {
                "code": 0,
                    "msg": "OK",
                    "data": {
                        "total": 39,
                        "rows": [
                        {
                            "orderID": "7d5a39d6-ff14-4428-b9e1-1fcf1800d6ac",
                            "clOrdID": "e422be37-074c-403d-aac8-ad94827f60c1",
                            "symbol": "BTCUSD",
                            "side": "Sell",
                            "orderType": "Limit",
                            "actionTimeNs": 1577523473419470300,
                            "priceEp": 75720000,
                            "price": null,
                            "orderQty": 12,
                            "displayQty": 0,
                            "timeInForce": "GoodTillCancel",
                            "reduceOnly": false,
                            "takeProfitEp": 0,
                            "takeProfit": null,
                            "stopLossEp": 0,
                            ...
                            "ordStatus": "Filled",
                        }
                    ]
                }
            }
        """
        params = {
            "symbol": symbol,
            "ordStatus": "Filled"
        }
        response = self._signed_request(self._path_config.get("get_closed_orders").get("method"),
                                        self._path_config.get("get_closed_orders").get("path"),
                                        params)
        return response

    def get_open_orders(self, symbol):
        """
        API to get all open orders by symbol
        https://github.com/phemex/phemex-api-docs/blob/master/Public-Contract-API-en.md#queryopenorder

        :param symbol: currency symbol
        :return: {
                "code": 0,
                    "msg": "",
                    "data": {
                        "rows": [
                        {
                            "bizError": 0,
                            "orderID": "9cb95282-7840-42d6-9768-ab8901385a67",
                            "clOrdID": "7eaa9987-928c-652e-cc6a-82fc35641706",
                            "symbol": "BTCUSD",
                            "side": "Buy",
                            "actionTimeNs": 1580533011677666800,
                            "transactTimeNs": 1580533011677666800,
                            "orderType": null,
                            "priceEp": 84000000,
                            "price": 8400,
                            "orderQty": 1,
                            "displayQty": 1,
                            "timeInForce": null,
                            "reduceOnly": false,
                            "stopPxEp": 0,
                            "closedPnlEv": 0,
                            "closedPnl": 0,
                            "closedSize": 0,
                            "cumQty": 0,
                            "cumValueEv": 0,
                            "cumValue": 0,
                            "leavesQty": 0,
                            "leavesValueEv": 0,
                            "leavesValue": 0,
                            "stopPx": 0,
                            "stopDirection": "Falling",
                            "ordStatus": "Untriggered"
                        }
                    ]
                }
            }
        """
        params = {
            "symbol": symbol,
        }
        response = self._signed_request(self._path_config.get("get_open_orders").get("method"),
                                        self._path_config.get("get_open_orders").get("path"),
                                        params)
        return response

    def _signed_request(self, method, path, params=None, data=None):
        expiry = utils.get_current_timestamp() + 1
        query_params = '&'.join(['{}={}'.format(k, v) for k, v in params.items()]) if params else None
        if data:
            data = json.dumps(data, separators=(',', ':')) if data else None
        header = self._get_request_credentials(path, expiry, params=query_params, payload=data)
        if data:
            data = data.encode()
        response = self.send_request(method, path, header, query_params, data)
        return response

    def _get_sign(self, data):
        m = hmac.new(self.secret.encode("utf-8"), data.encode("utf-8"), hashlib.sha256)
        return m.hexdigest()

    def _get_request_credentials(self, path, expiry, **kwargs):
        signature = ""
        if kwargs.get("params"):
            signature = self._get_sign(path + kwargs.get("params") + str(expiry))
        elif kwargs.get("payload"):
            signature = self._get_sign(path + str(expiry) + kwargs.get("payload"))
        header = {
            "x-phemex-access-token": self.key,
            "x-phemex-request-signature": signature,
            "x-phemex-request-expiry": str(expiry),
            "Content-Type": "application/json"
        }
        return header
