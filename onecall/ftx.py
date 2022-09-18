import hmac
import hashlib
import logging
import pandas as pd
import time

from base import utils
from base.exchange import Exchange
from base import urls


class FTX(Exchange):

    def __init__(self, key=None, secret=None, **kwargs):
        if "base_url" not in kwargs:
            kwargs["base_url"] = urls.FTX_FUT_BASE_URL

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

        ts = self.__get_ftx_ts()
        header = self._get_request_credentials(ts, 'GET', "/positions")
        response = super().send_request(self, 'GET', "/positions", header)
        return response

    def cancel_orders(self, symbol):
        if not symbol:
            logging.error("get_position request must have a request")
            return None

        ts = self.__get_ftx_ts()
        payload = {
          "market": symbol,
        }
        header = self._get_request_credentials(ts, 'DELETE', "/orders", data=str(payload))
        response = super().send_request(self, 'DELETE', "/orders", header, data=payload)
        return response

    def get_data(self, symbol, interval=15, time_frame=[], is_dataframe=False):
        if not symbol:
            logging.error("get_position request must have a request")
            return None

        params = {
            "resolution": interval
        }

        if len(time_frame) == 2:
            params["start_time"] = int(time_frame[0].timestamp() * 1000)
            params["end_time"] = int(time_frame[1].timestamp() * 1000)

        path = f'/markets/{symbol}/candles'
        response = super().send_request(self, 'GET', path, params=params)
        if is_dataframe:
            try:
                return pd.DataFrame(response.get("result"))
            except :
                logging.error("failed to create dataframe")
        return response

    def get_orderbook(self, symbol, limit=100, is_dataframe=False):
        if not symbol:
            logging.error("get_position request must have a request")
            return None

        params = {
            "depth": limit
        }
        path = f'/markets/{symbol}/orderbook'
        response = super().send_request(self, 'GET', path, params=params)
        if is_dataframe:
            try:
                columns = ['price', 'QTY']
                df = pd.DataFrame(response["result"]["bids"], columns=columns)
                orderbook = df.append(pd.DataFrame(response["result"]["asks"], columns=columns), ignore_index=True)
                return orderbook
            except :
                logging.error("failed to create dataframe")
        return response

    def get_balance(self):
        ts = self.__get_ftx_ts()
        header = self._get_request_credentials(ts, 'GET', "/wallet/balances")
        response = super().send_request(self, 'GET', "/wallet/all_balances", header)
        return response

    def market_order(self, symbol, side, quantity):
        if not symbol:
            logging.error("market_order request must have a request")
            return None

        ts = self.__get_ftx_ts()
        payload = {
            "market": symbol,
            "side": side,
            "type": "market",
            "size": quantity,
        }
        header = self._get_request_credentials(ts, 'POST', "/orders", data=str(payload))
        response = super().send_request(self, 'POST', "/orders", header, data=payload)
        return response

    def limit_order(self, symbol, side, quantity, price, post_only=False):
        if not symbol:
            logging.error("limit_order request must have a request")
            return None

        ts = self.__get_ftx_ts()
        payload = {
            "market": symbol,
            "side": side,
            "price": price,
            "type": "limit",
            "size": quantity,
            "postOnly": post_only
        }
        header = self._get_request_credentials(ts, 'POST', "/orders", data=str(payload))
        response = super().send_request(self, 'POST', "/orders", header, data=payload)
        return response

    def get_closed_orders(self):
        ts = self.__get_ftx_ts()
        header = self._get_request_credentials(ts, 'GET', "/orders/history")
        response = super().send_request(self, 'GET', "/orders/history", header)
        return list(filter(lambda order: order.get("status", "") == "closed", response.get("result", [])))

    def get_open_orders(self, symbol):
        ts = self.__get_ftx_ts()
        header = self._get_request_credentials(ts, 'GET', "/orders")
        response = super().send_request(self, 'GET', "/orders", header)
        return response

    def __signed_request(self, method, url, params=None, data=None):
        header, singed_params = self._get_request_credentials(params)
        response = super().send_request(self, method, url, header, singed_params, data)
        return response

    def _get_sign(self, data):
        m = hmac.new(super().secret.encode("utf-8"), str(data).encode("utf-8"), hashlib.sha256)
        return m.hexdigest()

    def _get_request_credentials(self, ts, method, path, params="", data=""):
        sign_string = f'{ts}{method}{path}{params}{data}'
        sing = self._get_sign(sign_string)
        header = {
            "FTX-KEY": super().key,
            "FTX-SIGN": sing,
            "FTX-TS": ts
        }
        return header

    def __get_ftx_ts(self):
        return int(time.time() * 1000)