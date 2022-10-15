import hmac
import hashlib
import logging
import pandas as pd
import base64
import uuid
import json
from urllib.parse import urljoin

from onecall.base import utils
from onecall.base.exchange import Exchange
from onecall.base import urls


class Kucoin(Exchange):
    """
    Kucoin API class
    """
    # interval
    INTERVAL_1 = 15
    INTERVAL_15 = 15
    INTERVAL_30 = 30
    INTERVAL_60 = 60
    INTERVAL_120 = 120
    INTERVAL_240 = 240
    INTERVAL_480 = 480
    INTERVAL_720 = 720
    INTERVAL_1440 = 1440
    INTERVAL_10080 = 10080

    # Constants for Order side
    BUY_SIDE = "buy"
    SELL_SIDE = "sell"

    def __init__(self, key=None, secret=None, passphrase=None, debug=False, **kwargs):
        """
        Kucoin API class
        https://docs.kucoin.com/futures/#general

        :param key: api key
        :param secret: secret key
        :param passphrase: passphrase
        :param debug: flag to switch to test env
        """
        self._path_config = {
            "get_positions": {"method": 'GET', "path": "/api/v1/position", "rate_limit": 50},
            "cancel_orders": {"method": "DELETE", "path": "/api/v1/orders", "rate_limit": 50},
            "get_data": {"method": "GET", "path": "/api/v1/kline/query", "rate_limit": 50},
            "get_orderbook": {"method": "GET", "path": "/api/v1/level2/snapshot", "rate_limit": 50},
            "get_balance": {"method": "GET", "path": "/api/v1/account-overview", "rate_limit": 50},
            "market_order": {"method": "POST", "path": "/api/v1/orders", "rate_limit": 50},
            "limit_order": {"method": "POST", "path": "/api/v1/orders", "rate_limit": 50},
            "get_closed_orders": {"method": "GET", "path": "/api/v1/orders", "rate_limit": 50},
            "get_open_orders": {"method": "GET", "path": "/api/v1/orders", "rate_limit": 50}
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
        https://docs.kucoin.com/futures/?lang=en_US#get-position-details

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
        https://docs.kucoin.com/futures/?lang=en_US#limit-order-mass-cancelation

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
        https://docs.kucoin.com/futures/?lang=en_US#get-k-line-data-of-contract

        :param symbol: symbol
        :param interval: chart interval
        :keyword from: start time
        :keyword ti: end time
        :keyword is_dataframe: convert data to dataframe or not
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
        if kwargs.get("is_dataframe", None) and response.get("data"):
            try:
                columns = ['Time', 'Entry price', 'Highest price', 'Lowest price', 'Close price', 'Volume']
                return pd.DataFrame(response["data"], columns=columns)
            except Exception as e:
                logging.error("failed to create dataframe : ", e)
        return response

    def get_orderbook(self, symbol: str, **kwargs):
        """
        Get orderbook
        https://docs.kucoin.com/futures/?lang=en_US#order-book

        :param symbol: contract symbol
        :keyword is_dataframe: convert data to dataframe or not
        :return: list of list/ pandas dataframe
        """
        params = {
            "symbol": symbol,
            **kwargs
        }
        response = self.__signed_request(self._path_config.get("get_orderbook").get("method"),
                                         self._path_config.get("get_orderbook").get("path"),
                                         params)
        if kwargs.get("is_dataframe", None) and response.get("data"):
            try:
                columns = ['price', 'QTY']
                bid = pd.DataFrame(response["data"]["bids"], columns=columns)
                bid["type"] = ["bid" for i in range(0, bid.shape[0])]
                ask = pd.DataFrame(response["data"]["asks"], columns=columns)
                ask["type"] = ["ask" for i in range(0, ask.shape[0])]
                df = pd.concat([bid, ask], ignore_index=True)
                return df
            except Exception as e:
                logging.error("failed to create dataframe: ", e)
        return response

    def get_balance(self):
        """
        API to get future account balance
        https://docs.kucoin.com/futures/?lang=en_US#get-account-overview

        :return: list
        """
        response = self.__signed_request(self._path_config.get("get_balance").get("method"),
                                         self._path_config.get("get_balance").get("path"))
        return response

    def market_order(self, symbol: str, side: str, quantity: float, leverage="5", **kwargs):
        """
        API to place market order
        https://docs.kucoin.com/futures/?lang=en_US#place-an-order

        :param symbol: symbol
        :param side: buy/sell
        :param quantity: order quantity
        :param leverage: Leverage of the order
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
            "leverage": leverage,
            **kwargs
        }
        response = self.__signed_request(self._path_config.get("market_order").get("method"),
                                         self._path_config.get("market_order").get("path"),
                                         data=payload)
        return response

    def limit_order(self, symbol: str, side: str, price: str, quantity: int, **kwargs):
        """
        API to place limit order
        https://docs.kucoin.com/futures/?lang=en_US#place-an-order

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
        https://docs.kucoin.com/futures/?lang=en_US#get-order-list

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
        params = {
            "status": "done",
            **kwargs
        }
        response = self.__signed_request(self._path_config.get("get_closed_orders").get("method"),
                                         self._path_config.get("get_closed_orders").get("path"),
                                         params)
        return response

    def get_open_orders(self, **kwargs):
        """
        API to get open orders
        https://docs.kucoin.com/futures/?lang=en_US#get-order-list

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

    def __signed_request(self, method, url, params=None, data=None):
        param_data = ""
        if params:
            strl = []
            for key in sorted(params):
                strl.append("{}={}".format(key, params[key]))
            param_data += '&'.join(strl)
            url += '?' + param_data
        if data:
            data = json.dumps(data)
        header = self._get_request_credentials(method, url, data=data)
        response = self.send_request(method, url, header, data=data)
        return response

    def _get_sign(self, str_to_sign):
        signature = base64.b64encode(hmac.new(self.secret.encode('utf-8'), str_to_sign.encode('utf-8'),
                                              hashlib.sha256).digest())
        passphrase = base64.b64encode(hmac.new(self.secret.encode('utf-8'), self.pass_phrase.encode('utf-8'),
                                               hashlib.sha256).digest())
        return signature, passphrase

    def _get_request_credentials(self, method, path, **kwargs):
        timestamp = utils.get_current_timestamp()
        if kwargs.get("data"):
            sign_string = str(timestamp)+method+path+kwargs["data"]
        else:
            sign_string = str(timestamp)+method+path
        sign, pass_phrase = self._get_sign(sign_string)
        headers = {
            "KC-API-SIGN": sign,
            "KC-API-TIMESTAMP": str(timestamp),
            "KC-API-KEY": self.key,
            "KC-API-PASSPHRASE": pass_phrase,
            "KC-API-KEY-VERSION": "2",
            "Content-Type": "application/json"
        }
        return headers
