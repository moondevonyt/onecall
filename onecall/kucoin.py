import hmac
import hashlib
import logging
import pandas as pd
import base64

from base import utils
from base.exchange import Exchange


class Kucoin(Exchange):

    def __init__(self, key=None, secret=None, **kwargs):
        if "base_url" not in kwargs:
            kwargs["base_url"] = "https://api-futures.kucoin.com"

        super().__init__(key, secret, **kwargs)

    def get_positions(self, symbol):
        """
        API to get current positions in future
        :param symbol: symbol
        :return:
        """
        if not symbol:
            logging.error("get_position request must have a request")
            return None

        params = {
            "symbol": symbol
        }
        header = self._get_request_credentials('GET', "/api/v1/position")
        response = super().send_request(self, 'GET', "/api/v1/position", header, params)
        return response

    def cancel_orders(self, symbol):
        """
        API to cancel all the active orders
        :param symbol:
        :return:
        """
        if not symbol:
            logging.error("cancel_order request must have a request")
            return None

        params = {
            "symbol": symbol
        }
        header = self._get_request_credentials("DELETE", "/api/v1/orders")
        response = super().send_request(self, 'DELETE', "/api/v1/orders", header, params)
        return response

    def get_data(self, symbol, interval=15, time_frame=[], limit=500, is_dataframe=False):
        """
        API to get OHLCV data
        :param symbol:
        :param interval:
        :param time_frame:
        :param limit:
        :param is_dataframe:
        :return: list of list/ pandas dataframe
        """
        if not symbol:
            logging.error("get_data request must have a request")
            return None

        if not interval:
            logging.error("get_data request must have a interval parameter")
            return None

        params = {
            "symbol": symbol,
            "granularity": interval
        }

        if len(time_frame) == 2:
            params["from"] = time_frame[0].timestamp() * 1000
            params["to"] = time_frame[1].timestamp() * 1000

        header = self._get_request_credentials("GET", "/api/v1/kline/query")
        response = super().send_request(self, 'GET', "/api/v1/kline/query", header, params)
        if is_dataframe:
            try:
                columns = ['Time', 'Entry price', 'Highest price', 'Lowest price', 'Close price', 'Volume']
                return pd.DataFrame(response["data"], columns=columns)
            except :
                logging.error("failed to create dataframe")
        return response

    def get_orderbook(self, symbol, is_dataframe=False):
        """
        Get orderbook
        :param symbol:
        :param limit:
        :param is_dataframe:
        :return:
        """
        if not symbol:
            logging.error("cancel_order request must have a request")
            return None

        params = {
            "symbol": symbol
        }
        header = self._get_request_credentials("GET", "/api/v1/level2/snapshot")
        response = super().send_request(self, 'GET', "/api/v1/level2/snapshot", header, params)
        if is_dataframe:
            try:
                columns = ['price', 'QTY']
                df = pd.DataFrame(response["data"]["bids"], columns=columns)
                orderbook = df.append(pd.DataFrame(response["data"]["asks"], columns=columns), ignore_index=True)
                return orderbook
            except:
                logging.error("failed to create dataframe")
        return response

    def get_balance(self):
        """
        API to get future account balance
        :return: list
        """
        header = self._get_request_credentials("GET", "/api/v1/account-overview")
        response = self.__signed_request('GET', "/api/v1/account-overview", header)
        return response

    def market_order(self, client_id, symbol, side, quantity):
        """
        API to place market order
        :param symbol:
        :param side:
        :param quantity:
        :param client_id:
        :return:
        """
        if not symbol:
            logging.error("market_order request must have a request")
            return None

        payload = {
            "clientOid": client_id,
            "symbol": symbol,
            "side": side,
            "type": "market",
            "size": quantity
        }
        header = self._get_request_credentials("POST", "/api/v1/orders", str(payload))
        response = super().send_request(self, 'POST', "/api/v1/orders", header, data=payload)
        return response

    def limit_order(self, client_id, symbol, side, price, quantity, time_in_force="GTC", post_only=False):
        """
        API to place limit order
        :param client_id:
        :param symbol:
        :param side:
        :param quantity:
        :param time_in_force:
        :param post_only:
        :param price:
        :return:
        """
        if not symbol:
            logging.error("market_order request must have a request")
            return None

        payload = {
            "clientOid": client_id,
            "symbol": symbol,
            "side": side,
            "price": price,
            "type": "limit",
            "size": quantity,
            "timeInForce": time_in_force,
            "postOnly": post_only
        }
        header = self._get_request_credentials("POST", "/api/v1/orders", str(payload))
        response = super().send_request(self, 'POST', "/api/v1/orders", header, data=payload)
        return response

    def get_closed_orders(self):
        """
        API to get closed orders
        :return:
        """
        header = self._get_request_credentials("GET", "/api/v1/fills")
        response = super().send_request(self, 'GET', "/api/v1/fills", header)
        return response

    def get_open_orders(self):
        params = {
            "status": "active"
        }
        header = self._get_request_credentials("GET", "/api/v1/orders")
        response = super().send_request(self, 'GET', "/api/v1/orders", header, params)
        return response

    def __signed_request(self, method, url, params=None, data=None):
        header, singed_params = self._get_request_credentials(params)
        response = super().send_request(self, method, url, header, singed_params, data)
        return response

    def _get_sign(self, str_to_sign):
        signature = base64.b64encode(hmac.new(super().secret.encode('utf-8'), str_to_sign.encode('utf-8'),
                                              hashlib.sha256).digest())
        passphrase = base64.b64encode(hmac.new(super().secret.encode('utf-8'), super().pass_phrase.encode('utf-8'),
                                               hashlib.sha256).digest())
        return signature, passphrase

    def _get_request_credentials(self, method, path, data=""):
        sign, pass_phrase = self._get_sign(utils.get_current_timestamp()+method+path+data)
        headers = {
            "KC-API-SIGN": sign,
            "KC-API-TIMESTAMP": str(utils.get_current_timestamp()),
            "KC-API-KEY": super().key,
            "KC-API-PASSPHRASE": pass_phrase,
            "KC-API-KEY-VERSION": "2"
        }
        return headers
