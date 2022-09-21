import hmac
import hashlib
import logging
import pandas as pd
import base64
import uuid
import json
from urllib.parse import urljoin

from base import utils
from base.exchange import Exchange
from base import urls


class Kucoin(Exchange):

    def __init__(self, key=None, secret=None, passphrase=None, debug=False, **kwargs):
        self._path_config = {
            "get_positions": {"method": "GET", "path": "/api/v2/symbol-position", "rate_limit": 50},
            "cancel_orders": {"method": "DELETE", "path": "/api/v2/orders", "rate_limit": 50},
            "get_data": {"method": "GET", "path": "/api/v2/kline/query", "rate_limit": 50},
            "get_orderbook": {"method": "GET", "path": "/api/v2/order-book", "rate_limit": 50},
            "get_balance": {"method": "GET", "path": "/api/v2/account-overview", "rate_limit": 50},
            "market_order": {"method": "POST", "path": "/api/v2/order", "rate_limit": 50},
            "limit_order": {"method": "POST", "path": "/api/v2/order", "rate_limit": 50},
            "get_closed_orders": {"method": "GET", "path": "/api/v2/orders/history", "rate_limit": 50},
            "get_open_orders": {"method": "GET", "path": "/api/v2/orders/all-active", "rate_limit": 50}
        }
        if not debug:
            kwargs["base_url"] = urls.KUCOIN_FUT_BASE_URL
        else:
            kwargs["base_url"] = urls.KUCOIN_FUT_TEST_BASE_URL
        super().__init__(key, secret, passphrase, **kwargs)
        return

    def get_positions(self, symbol: str):
        """
        API to get current positions in future

        :param symbol: symbol
        :return: {
            "code": "200000",
            "data": {
            "id": "5e81a7827911f40008e80715",//Position ID
            "symbol": "XBTUSDTM",//Symbol
            "autoDeposit": False,//Auto deposit margin or not
            "maintMarginReq": 0.005,//Maintenance margin requirement
            "riskLimit": 2000000,//Risk limit
            "realLeverage": 5.0,//Leverage o the order
            "crossMode": False,//Cross mode or not
            "delevPercentage": 0.35,//ADL ranking percentile
            "openingTimestamp": 1623832410892,//Open time
            "currentTimestamp": 1623832488929,//Current timestamp
            "currentQty": 1,//Current postion quantity
            ...
            }
        }
        """
        params = {
            "symbol": symbol
        }
        response = self.__signed_request(self._path_config.get("get_positions").get("method"),
                                         self._path_config.get("get_positions").get("path"),
                                         params)
        return response

    def cancel_orders(self, symbol: str):
        """
        API to cancel all the active orders

        :param symbol: symbol
        :return:  {
            "code": "200000",
            "data": {
              "cancelledOrderIds": [
                "5c52e11203aa677f33e493fb",
                "5c52e12103aa677f33e493fe",
              ]
            }
          }
        """
        params = {
            "symbol": symbol
        }
        response = self.__signed_request(self._path_config.get("cancel_orders").get("method"),
                                         self._path_config.get("cancel_orders").get("path"),
                                         params)
        return response

    def get_data(self, symbol: str, interval: int, **kwargs):
        """
        API to get OHLCV data

        :param symbol: symbol
        :param interval: chart interval
        :keyword from: start time
        :keyword ti: end time
        :return: list of list/ pandas dataframe
        """
        params = {
            "symbol": symbol,
            "granularity": interval,
            **kwargs
        }
        response = self.__signed_request(self._path_config.get("get_data").get("method"),
                                         self._path_config.get("get_data").get("path"),
                                         params)
        if kwargs.get("is_dataframe", None):
            try:
                columns = ['Time', 'Entry price', 'Highest price', 'Lowest price', 'Close price', 'Volume']
                return pd.DataFrame(response["data"], columns=columns)
            except Exception as e:
                logging.error("failed to create dataframe : ", e)
        return response

    def get_orderbook(self, symbol: str, **kwargs):
        """
        Get orderbook

        :param symbol:
        :keyword is_dataframe:
        :return: list of list/ pandas dataframe
        """
        params = {
            "symbol": symbol,
            **kwargs
        }
        response = self.__signed_request(self._path_config.get("get_orderbook").get("method"),
                                         self._path_config.get("get_orderbook").get("path"),
                                         params)
        if kwargs.get("is_dataframe", None):
            try:
                columns = ['price', 'QTY']
                df = pd.DataFrame(response["data"]["bids"], columns=columns)
                orderbook = df.append(pd.DataFrame(response["data"]["asks"], columns=columns), ignore_index=True)
                return orderbook
            except Exception as e:
                logging.error("failed to create dataframe: ", e)
        return response

    def get_balance(self):
        """
        API to get future account balance

        :return: list
        """
        response = self.__signed_request(self._path_config.get("get_balance").get("method"),
                                         self._path_config.get("get_balance").get("path"))
        return response

    def market_order(self, symbol: str, side: str, quantity: float, **kwargs):
        """
        API to place market order
        https://docs.kucoin.com/futures/#place-an-orde

        :param symbol: symbol
        :param side: buy/sell
        :param quantity: order quantity
        :return: {
            "code": "200000",
            "data": {
              "orderId": "5bd6e9286d99522a52e458de"
              }
          }
        """
        payload = {
            "clientOid": str(uuid.uuid1()),
            "symbol": symbol,
            "side": side,
            "type": "market",
            "size": quantity,
            **kwargs
        }
        response = self.__signed_request(self._path_config.get("market_order").get("method"),
                                         self._path_config.get("market_order").get("path"),
                                         data=payload)
        return response

    def limit_order(self, symbol: str, side: str, price: str, quantity: int, **kwargs):
        """
        API to place limit order
        https://docs.kucoin.com/futures/#place-an-orde

        :param symbol: symbol
        :param side: buy/sell
        :param quantity: order quantity
        :param price: order price
        :return: {
            "code": "200000",
            "data": {
              "orderId": "5bd6e9286d99522a52e458de"
              }
          }
        """
        payload = {
            "clientOid": str(uuid.uuid1()),
            "symbol": symbol,
            "side": side,
            "price": price,
            "type": "limit",
            "size": quantity,
            **kwargs
        }
        response = self.__signed_request(self._path_config.get("limit_order").get("method"),
                                         self._path_config.get("limit_order").get("path"),
                                         data=payload)
        return response

    def get_closed_orders(self, **kwargs):
        """
        API to get closed orders
        https://docs.kucoin.com/futures/#get-fills

        :return: {
                "code": "200000",
                "data": {
                  "currentPage":1,
                  "pageSize":1,
                  "totalNum":251915,
                  "totalPage":251915,
                  "items":[
                      {
                        "symbol": "XBTUSDM",  //Symbol of the contract
                        "tradeId": "5ce24c1f0c19fc3c58edc47c",  //Trade ID
                        "orderId": "5ce24c16b210233c36ee321d",  // Order ID
                        "side": "sell",  //Transaction side
                        "liquidity": "taker",  //Liquidity- taker or maker
                        "forceTaker": true, //Whether to force processing as a taker
                        "price": "8302",  //Filled price
                        "size": 10,  //Filled amount
                        "value": "0.001204529",  //Order value
                        ...
                      }
                  ]
                }
            }
        """
        response = self.__signed_request(self._path_config.get("get_closed_orders").get("method"),
                                         self._path_config.get("get_closed_orders").get("path"),
                                         kwargs)
        return response

    def get_open_orders(self, **kwargs):
        """
        API to get open orders
        https://docs.kucoin.com/futures/#stop-order-mass-cancelation

        :return:
        """
        params = {
            "status": "active",
            **kwargs
        }
        response = self.__signed_request(self._path_config.get("get_open_orders").get("method"),
                                         self._path_config.get("get_open_orders").get("path"),
                                         params)
        return response

    def __signed_request(self, method, uri, params=None):
        uri_path = uri
        data_json = ''
        if method in ['GET', 'DELETE']:
            if params:
                strl = []
                for key in sorted(params):
                    strl.append("{}={}".format(key, params[key]))
                data_json += '&'.join(strl)
                uri += '?' + data_json
                uri_path = uri
        else:
            if params:
                data_json = json.dumps(params)
                uri_path = uri + data_json

        now_time = int(utils.get_current_timestamp()) * 1000
        str_to_sign = str(now_time) + method + uri_path
        sign = base64.b64encode(hmac.new(self.secret.encode('utf-8'), str_to_sign.encode('utf-8'),
                                         hashlib.sha256).digest())
        passphrase = base64.b64encode(
            hmac.new(self.secret.encode('utf-8'), self.pass_phrase.encode('utf-8'), hashlib.sha256).digest())
        headers = {
            "KC-API-SIGN": sign,
            "KC-API-TIMESTAMP": str(now_time),
            "KC-API-KEY": self.key,
            "KC-API-PASSPHRASE": passphrase,
            "Content-Type": "application/json",
            "KC-API-KEY-VERSION": "2"
        }
        response = self.send_request(method, uri, headers, data=data_json)
        return response

    # def __signed_request(self, method, url, params=None, data=None):
    #     param_data = ""
    #     if params:
    #         strl = []
    #         for key in sorted(params):
    #             strl.append("{}={}".format(key, params[key]))
    #         param_data += '&'.join(strl)
    #         url += '?' + param_data
    #     if data:
    #         data = json.dumps(data)
    #     header = self._get_request_credentials(method, url, params=params, data=data)
    #     response = self.send_request(method, url, header, params, data)
    #     return response

    # def _get_sign(self, str_to_sign):
    #     signature = base64.b64encode(hmac.new(self.secret.encode('utf-8'), str_to_sign.encode('utf-8'),
    #                                           hashlib.sha256).digest())
    #     passphrase = base64.b64encode(hmac.new(self.secret.encode('utf-8'), self.pass_phrase.encode('utf-8'),
    #                                            hashlib.sha256).digest())
    #     return signature, passphrase

    # def _get_request_credentials(self, method, path, **kwargs):
    #     timestamp = utils.get_current_timestamp()
    #     sign_string = ""
    #     if kwargs.get("data"):
    #         sign_string = f'{str(timestamp)}{method}{path}{kwargs["data"]}'
    #     else:
    #         sign_string = str(timestamp) + method + path
    #     sign, pass_phrase = self._get_sign(sign_string)
    #     headers = {
    #         "KC-API-SIGN": sign,
    #         "KC-API-TIMESTAMP": str(timestamp),
    #         "KC-API-KEY": self.key,
    #         "KC-API-PASSPHRASE": pass_phrase,
    #         "KC-API-KEY-VERSION": "2"
    #     }
    #     return headers
