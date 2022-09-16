import hmac
import hashlib
import logging
from base import utils
from base.exchange import Exchange
import pandas as pd


class Binance(Exchange):

    def __init__(self, key=None, secret=None, **kwargs):
        if "base_url" not in kwargs:
            kwargs["base_url"] = "https://fapi.binance.com"

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
            "symbol": symbol,
            "timestamp": utils.get_current_timestamp()
        }
        header, singed_params = self._get_request_credentials(params)
        response = super().send_request(self, 'GET', "/fapi/v2/positionRisk", header, singed_params)
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
            "symbol": symbol,
            "timestamp": utils.get_current_timestamp()
        }
        header, singed_params = self._get_request_credentials(params)
        response = super().send_request(self, 'DELETE', "/fapi/v1/batchOrders", header, singed_params)
        return response

    def get_data(self, symbol, interval, time_frame=[], limit=500, is_dataframe=False):
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
            "interval": interval,
            "timestamp": utils.get_current_timestamp(),
            "limit": limit
        }

        if len(time_frame) == 2:
            params["startTime"] = time_frame[0].timestamp() * 1000
            params["endTime"] = time_frame[1].timestamp() * 1000

        response = super().send_request(self, 'GET', "/fapi/v1/klines", params=params)
        if is_dataframe:
            try:
                columns = ['Open Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close time',
                           'Quote asset volume', 'Number of trades', 'Taker buy base asset volume',
                           'Taker buy quote asset volume']
                return pd.DataFrame(response, columns=columns)
            except :
                logging.error("failed to create dataframe")
        return response

    def get_orderbook(self, symbol, limit=500, is_dataframe=False):
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
            "symbol": symbol,
            "limit": limit
        }
        response = super().send_request(self, 'GET', "/fapi/v1/depth", params=params)
        if is_dataframe:
            try:
                columns = ['price', 'QTY']
                df = pd.DataFrame(response["bids"], columns=columns)
                orderbook = df.append(pd.DataFrame(response["asks"], columns=columns), ignore_index=True)
                return orderbook
            except:
                logging.error("failed to create dataframe")
        return response

    def get_balance(self):
        """
        API to get future account balance
        :return: list
        """
        params = {
            "timestamp": utils.get_current_timestamp()
        }
        response = self.__signed_request('GET', "/fapi/v2/balance ", params)
        return response

    def market_order(self, symbol, side, quantity):
        """
        API to place market order
        :param symbol:
        :param side:
        :param quantity:
        :return:
        """
        if not symbol:
            logging.error("market_order request must have a request")
            return None

        params = {
            "symbol": symbol,
            "side": side,
            "type": "MARKET",
            "quantity": quantity,
            "timestamp": utils.get_current_timestamp()
        }
        header, singed_params = self._get_request_credentials(params)
        response = super().send_request(self, 'POST', "/fapi/v1/order", header, singed_params)
        return response

    def limit_order(self, symbol, side, quantity, price, time_in_force="GTC"):
        """
        API to place limit order
        :param symbol:
        :param side:
        :param quantity:
        :param price:
        :param time_in_force:
        :return:
        """
        if not symbol:
            logging.error("market_order request must have a request")
            return None

        params = {
            "symbol": symbol,
            "side": side,
            "type": "LIMIT",
            "quantity": quantity,
            "price": price,
            "timeInForce": time_in_force,
            "timestamp": utils.get_current_timestamp()
        }
        header, singed_params = self._get_request_credentials(params)
        response = super().send_request(self, 'POST', "/fapi/v1/order", header, singed_params)
        return response

    def get_closed_orders(self, symbol):
        if not symbol:
            logging.error("get_closed_orders request must have a request")
            return None

        params = {
            "symbol": symbol,
            "timestamp": utils.get_current_timestamp()
        }
        header, singed_params = self._get_request_credentials(params)
        response = super().send_request(self, 'GET', "/fapi/v1/allOrders", header, singed_params)
        return list(map(lambda order: order["status"] == "FILLED", response))

    def get_open_orders(self, symbol):
        params = {
            "timestamp": utils.get_current_timestamp()
        }
        if symbol:
            params["symbol"] = symbol
        header, singed_params = self._get_request_credentials(params)
        response = super().send_request(self, 'GET', "/fapi/v1/openOrders", header, singed_params)
        return response

    def __signed_request(self, method, url, params=None, data=None):
        header, singed_params = self._get_request_credentials(params)
        response = super().send_request(self, method, url, header, singed_params, data)
        return response

    def _get_sign(self, data):
        m = hmac.new(super().secret.encode("utf-8"), str(data).encode("utf-8"), hashlib.sha256)
        return m.hexdigest()

    def _get_request_credentials(self, params):
        header = {"X-MBX-APIKEY": super().key}
        singed_params = {**params, "signature": self._get_sign(params)}
        return header, singed_params
