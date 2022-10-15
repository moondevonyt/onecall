import hmac
import hashlib
import json
import logging
import pandas as pd
from urllib.parse import urlencode

from onecall.base import utils
from onecall.base.exchange import Exchange
from onecall.base import urls


class Bybit(Exchange):
    """
    Bybit API Class
    """
    INTERVAL_1m = '1'
    INTERVAL_3m = '3'
    INTERVAL_5m = '5'
    INTERVAL_15m = '15'
    INTERVAL_30m = '30'
    INTERVAL_1H = '60'
    INTERVAL_2H = '120'
    INTERVAL_4H = '240'
    INTERVAL_6H = '360'
    INTERVAL_12H = '720'
    INTERVAL_1D = 'D'
    INTERVAL_1W = 'W'
    INTERVAL_1M = 'M'

    # Constants for Order side
    BUY_SIDE = 'Buy'
    SELL_SIDE = 'Sell'

    def __init__(self, key=None, secret=None, debug=False, **kwargs):
        """
        Bybit API class
        https://bybit-exchange.github.io/docs/futuresV2/linear/#t-introduction

        :param key: api key
        :param secret: secret key
        :param debug: flag to switch to test env
        """
        self._path_config = {
            "get_positions": {"method": "GET", "path": "/private/linear/position/list", "rate_limit": 50},
            "cancel_active_orders": {"method": "POST", "path": "/private/linear/order/cancel-all", "rate_limit": 50},
            "cancel_stop_orders": {"method": "POST", "path": "/private/linear/stop-order/cancel-all", "rate_limit": 50},
            "get_data": {"method": "GET", "path": "/public/linear/kline", "rate_limit": 50},
            "get_orderbook": {"method": "GET", "path": "/v2/public/orderBook/L2", "rate_limit": 50},
            "get_balance": {"method": "GET", "path": "/v2/private/wallet/balance", "rate_limit": 50},
            "market_order": {"method": "POST", "path": "/private/linear/order/create", "rate_limit": 50},
            "limit_order": {"method": "POST", "path": "/private/linear/order/create", "rate_limit": 50},
            "get_closed_orders": {"method": "GET", "path": "/private/linear/order/list", "rate_limit": 50},
            "get_open_orders": {"method": "GET", "path": "/private/linear/order/search", "rate_limit": 50}
        }
        self.recv_window = "5000"
        if not debug:
            kwargs["base_url"] = urls.BYBIT_FUT_BASE_URL
        else:
            kwargs["base_url"] = urls.BYBIT_FUT_TEST_BASE_URL
        super().__init__(key, secret, **kwargs)
        return

    def get_positions(self, symbol: str):
        """
        API to get current positions
        https://bybit-exchange.github.io/docs/futuresV2/linear/#t-position

        :param symbol: contract symbol
        :return: {
            "ret_code": 0,
            "ret_msg": "OK",
            "ext_code": "",
            "ext_info": "",
            "result": [
                {
                    "user_id": 533285,
                    "symbol": "XRPUSDT",
                    "side": "Buy",
                    "size": 0,
                    "position_value": 0,
                    "entry_price": 0,
                    "liq_price": 0,
                    "bust_price": 0,
                    "leverage": 5,
                    "auto_add_margin": 0,
                    "is_isolated": true,
                    "position_margin": 0,
                    }
                ]
            }
        """
        params = {
            "symbol": symbol,
        }
        response = self._signed_request(self._path_config.get("get_positions").get("method"),
                                        self._path_config.get("get_positions").get("path"),
                                        params)
        return response

    def cancel_orders(self, symbol: str):
        """
        API to cancel all orders
        https://bybit-exchange.github.io/docs/futuresV2/linear/#t-cancelallactive
        https://bybit-exchange.github.io/docs/futuresV2/linear/#t-cancelallcond

        :param symbol: symbol
        :return: {"active_order": {active_response}, "conditional_order": {conditional_response}}
        """
        payload = {
            "symbol": symbol,
        }
        active_response = self._signed_request(self._path_config.get("cancel_active_orders").get("method"),
                                               self._path_config.get("cancel_active_orders").get("path"),
                                               data=payload)
        conditional_response = self._signed_request(self._path_config.get("cancel_stop_orders").get("method"),
                                                    self._path_config.get("cancel_stop_orders").get("path"),
                                                    data=payload)
        return {"active_order": active_response, "conditional_order": conditional_response}

    def get_data(self, symbol: str, interval: str, start_time: int, **kwargs):
        """
        API to get OHLCV data
        https://bybit-exchange.github.io/docs/futuresV2/linear/#t-querykline

        :param symbol: symbol
        :param interval: interval
        :param start_time: start time
        :keyword limit: data limit
        :keyword is_dataframe: convert the data to pandas dataframe
        :return: list of list/ pandas dataframe
        """
        params = {
            "symbol": symbol,
            "interval": interval,
            "from": start_time,
            **kwargs
        }
        response = self._signed_request(self._path_config.get("get_data").get("method"),
                                        self._path_config.get("get_data").get("path"),
                                        params)
        if kwargs.get("is_dataframe"):
            try:
                return pd.DataFrame(response["result"])
            except Exception as e:
                logging.error("failed to create dataframe: ", e)
        return response

    def get_orderbook(self, symbol, is_dataframe=False):
        """
        API to get orderbook
        https://bybit-exchange.github.io/docs/futuresV2/linear/#t-orderbook

        :param symbol: symbol
        :param is_dataframe: convert the data to pandas dataframe
        :return: {
                "ret_code": 0,
                "ret_msg": "OK",
                "ext_code": "",
                "ext_info": "",
                "result": [
                    {
                        "symbol": "BTCUSD",
                        "price": "9487",
                        "size": 336241,
                        "side": "Buy"
                    },
                    {
                        "symbol": "BTCUSD",
                        "price": "9487.5",
                        "size": 522147,
                        "side": "Sell"
                    }
                ],
                "time_now": "1567108756.834357"
            }
        """
        params = {
            "symbol": symbol
        }
        response = self._signed_request(self._path_config.get("get_orderbook").get("method"),
                                        self._path_config.get("get_orderbook").get("path"),
                                        params)
        if is_dataframe:
            try:
                return pd.DataFrame(response["result"])
            except Exception as e:
                logging.error("failed to create dataframe:", e)
        return response

    def get_balance(self):
        """
        API to get balance
        https://bybit-exchange.github.io/docs/futuresV2/inverse/#t-balance

        :return: {
                "ret_code": 0,
                "ret_msg": "OK",
                "ext_code": "",
                "ext_info": "",
                "result": {
                    "BTC": {
                        "equity": 1002,
                        "available_balance": 999.99987471,
                        "used_margin": 0.00012529,
                        "order_margin": 0.00012529,
                        "position_margin": 0,
                        "occ_closing_fee": 0,
                        "occ_funding_fee": 0,
                        "wallet_balance": 1000,
                        "realised_pnl": 0,
                        "unrealised_pnl": 2,
                        "cum_realised_pnl": 0,
                        "given_cash": 0,
                        "service_cash": 0
                    }
                },
                "time_now": "1578284274.816029",
                "rate_limit_status": 98,
                "rate_limit_reset_ms": 1580885703683,
                "rate_limit": 100
            }
        """
        response = self._signed_request(self._path_config.get("get_balance").get("method"),
                                        self._path_config.get("get_balance").get("path"))
        return response

    def market_order(self, symbol, side, quantity, time_in_force="GoodTillCancel", reduce_only=False,
                     close_on_trigger=False, **kwargs):
        """
        API to create market order
        https://bybit-exchange.github.io/docs/futuresV2/linear/#t-placeactive

        :param symbol: symbol
        :param side: buy/sell
        :param quantity: order quantity
        :param time_in_force: time in force
        :param reduce_only: your position can only reduce in size if this order is triggered
        :param close_on_trigger: For a closing order. It can only reduce your position, not increase it.
        :return: {
                "ret_code": 0,
                "ret_msg": "OK",
                "ext_code": "",
                "ext_info": "",
                "result": {
                    "user_id": 533285,
                    "order_id": "a1904030-f99c-4e35-9217-111591f08493",
                    "symbol": "BTCUSD",
                    "side": "Buy",
                    "order_type": "Limit",
                    "price": 20010,
                    "qty": 200,
                    ...
                },
                "time_now": "1655708182.071362",
                "rate_limit_status": 99,
                "rate_limit_reset_ms": 1655708182068,
                "rate_limit": 100
            }
        """
        payload = {
            "side": side,
            "symbol": symbol,
            "order_type": "Market",
            "qty": quantity,
            "time_in_force": time_in_force,
            "reduce_only": reduce_only,
            "close_on_trigger": close_on_trigger,
            **kwargs
        }
        response = self._signed_request(self._path_config.get("market_order").get("method"),
                                        self._path_config.get("market_order").get("path"),
                                        data=payload)
        return response

    def limit_order(self, symbol, side, quantity, price, time_in_force="GoodTillCancel", reduce_only=False,
                    close_on_trigger=False, **kwargs):
        """
        API to create limit order
        https://bybit-exchange.github.io/docs/futuresV2/linear/#t-placeactive

        :param symbol: symbol
        :param side: buy/sell
        :param quantity: order quantity
        :param price: order price
        :param time_in_force: time in force
        :param reduce_only: your position can only reduce in size if this order is triggered
        :param close_on_trigger: For a closing order. It can only reduce your position, not increase it.
        :return: {
                    "user_id": 533285,
                    "order_id": "a1904030-f99c-4e35-9217-111591f08493",
                    "symbol": "BTCUSD",
                    "side": "Buy",
                    "order_type": "Limit",
                    "price": 20010,
                    "qty": 200,
                    ...
                },
                "time_now": "1655708182.071362",
                "rate_limit_status": 99,
                "rate_limit_reset_ms": 1655708182068,
                "rate_limit": 100
            }
        """
        payload = {
            "side": side,
            "symbol": symbol,
            "order_type": "Limit",
            "qty": quantity,
            "price": price,
            "time_in_force": time_in_force,
            "reduce_only": reduce_only,
            "close_on_trigger": close_on_trigger,
            **kwargs
        }
        response = self._signed_request(self._path_config.get("limit_order").get("method"),
                                        self._path_config.get("limit_order").get("path"),
                                        data=payload)
        return response

    def get_closed_orders(self, symbol):
        """
        API to get closed orders
        https://bybit-exchange.github.io/docs/futuresV2/linear/#t-getactive

        :param symbol: symbols
        :return: {
                "ret_code": 0,
                "ret_msg": "OK",
                "ext_code": "",
                "ext_info": "",
                "result": [
                    {
                        "order_id": "8e7d1cd2-d7b3-4c61-87a9-a85a6e59cef8",
                        "user_id": 533285,
                        "symbol": "BITUSDT",
                        "side": "Buy",
                        "order_type": "Limit",
                        "price": 0.3,
                        "qty": 100,
                        "time_in_force": "GoodTillCancel",
                        "order_status": "New",
                        "last_exec_price": 0,
                    }
                ],
                "time_now": "1655863947.736147",
                "rate_limit_status": 599,
                "rate_limit_reset_ms": 1655863947734,
                "rate_limit": 600
            }
        """
        param = {
            "symbol": symbol
        }
        response = self._signed_request(self._path_config.get("get_closed_orders").get("method"),
                                        self._path_config.get("get_closed_orders").get("path"),
                                        param)
        if response.get("result"):
            return list(filter(lambda order: order.get("order_status") == "Filled",
                               response.get("result", {}).get("data")))
        return response

    def get_open_orders(self, symbol):
        """
        API to get open orders
        https://bybit-exchange.github.io/docs/futuresV2/linear/#t-queryactive

        :param symbol: symbol
        :return: {
                "ret_code": 0,
                "ret_msg": "OK",
                "ext_code": "",
                "ext_info": "",
                "result": [
                    {
                        "order_id": "8e7d1cd2-d7b3-4c61-87a9-a85a6e59cef8",
                        "user_id": 533285,
                        "symbol": "BITUSDT",
                        "side": "Buy",
                        "order_type": "Limit",
                        "price": 0.3,
                        "qty": 100,
                        "time_in_force": "GoodTillCancel",
                        "order_status": "New",
                        "last_exec_price": 0,
                    }
                ],
                "time_now": "1655863947.736147",
                "rate_limit_status": 599,
                "rate_limit_reset_ms": 1655863947734,
                "rate_limit": 600
            }
        """
        param = {
            "symbol": symbol
        }
        response = self._signed_request(self._path_config.get("get_open_orders").get("method"),
                                        self._path_config.get("get_open_orders").get("path"),
                                        param)
        return response

    def _signed_request(self, method, url, params=None, data=None):
        timestamp = str(utils.get_current_timestamp())
        if data:
            data["timestamp"] = timestamp
            data["api_key"] = self.key
        elif params:
            params["timestamp"] = timestamp
            params["api_key"] = self.key
        else:
            params = {"timestamp": timestamp, "api_key": self.key}
        sign = self._get_request_credentials(params=params, data=data)
        headers = {"Content-Type": "application/json"}
        if data:
            data = json.dumps(dict(data, **sign))
        else:
            params = urlencode(params) + "&sign=" + sign.get("sign")
        response = self.send_request(method, url, headers, params, data)
        return response

    def _get_sign(self, str_to_sign):
        sign = hmac.new(self.secret.encode("utf-8"), str_to_sign.encode("utf-8"), hashlib.sha256)
        return sign.hexdigest()

    def _get_request_credentials(self, params=None, data=None):
        if data:
            sign = self._get_sign(self._get_sign_string(data))
        else:
            sign = self._get_sign(self._get_sign_string(params))
        return {"sign": sign}

    def _get_sign_string(self, params):
        sign_string = ""
        for key in sorted(params.keys()):
            v = params[key]
            if isinstance(params[key], bool):
                if params[key]:
                    v = 'true'
                else:
                    v = 'false'
            sign_string += key + '=' + str(v) + '&'
        return sign_string[:-1]
