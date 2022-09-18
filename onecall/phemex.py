import hmac
import hashlib
import logging
import pandas as pd

from base import utils
from base.exchange import Exchange
from base import urls



class Phemex(Exchange):

    def __init__(self, key=None, secret=None, **kwargs):
        if "base_url" not in kwargs:
            kwargs["base_url"] = urls.PHEMEX_FUT_BASE_URL

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
        header = self._get_request_credentials("'accounts/positions'", params, utils.get_current_timestamp())
        response = super().send_request(self, 'GET', "'accounts/positions'", header, params)
        return response

    def cancel_orders(self, symbol, un_triggered=None):
        """
        API to cancel all orders
        :param symbol:
        :param un_triggered:
        :return:
        """
        if not symbol:
            logging.error("cancel_orders request must have a request")
            return None

        params = {
            "symbol": symbol,
            "untriggered": un_triggered
        }
        header = self._get_request_credentials("/spot/orders/all", params, utils.get_current_timestamp())
        response = super().send_request(self, 'DELETE', "/spot/orders/all", header, params)
        return response

    def get_data(self, symbol, is_dataframe=False):
        """

        :param symbol:
        :param is_dataframe:
        :return:
        """
        if not symbol:
            logging.error("get_data request must have a request")
            return None

        params = {
            "symbol": symbol
        }
        header = self._get_request_credentials("/md/spot/ticker/24hr", params, utils.get_current_timestamp())
        response = super().send_request(self, 'GET', "/md/spot/ticker/24hr", header, params)
        if is_dataframe:
            try:
                return pd.DataFrame(response["result"])
            except :
                logging.error("failed to create dataframe")
        return response

    def get_orderbook(self, symbol, is_dataframe=False):
        """
        API to get orderbook
        :param symbol:
        :param is_dataframe:
        :return:
        """
        if not symbol:
            logging.error("get_orderbook request must have a request")
            return None

        params = {
            "symbol": symbol
        }
        header = self._get_request_credentials("/md/orderbook", params, utils.get_current_timestamp())
        response = super().send_request(self, 'GET', "/md/orderbook", header, params)
        if is_dataframe:
            try:
                columns = ['price', 'QTY']
                df = pd.DataFrame(response["result"]["book"]["bids"], columns=columns)
                orderbook = df.append(pd.DataFrame(response["result"]["book"]["asks"], columns=columns), ignore_index=True)
                return orderbook
            except:
                logging.error("failed to create dataframe")
        return response

    def get_balance(self):
        header = self._get_request_credentials("accounts/accountPositions", "", utils.get_current_timestamp())
        response = super().send_request(self, 'GET', "accounts/accountPositions", header)
        return response["data"]

    def market_order(self, symbol, side, qty_type="ByBase", quote_qty_ev=0, base_qty_ev=0):
        if not symbol:
            logging.error("market_order request must have a request")
            return None

        payload = {
            "symbol": symbol,
            "side": side,
            "ordType": "Market",
            "qtyType": qty_type,
            "quoteQtyEv": quote_qty_ev,
            "baseQtyEv": base_qty_ev
        }
        header = self._get_request_credentials("/spot/orders", payload, utils.get_current_timestamp())
        response = super().send_request(self, 'POST', "/spot/orders", header, data=payload)
        return response

    def limit_order(self, symbol, side, qty_type="ByBase", quote_qty_ev=0, base_qty_ev=0,
                    time_i_force="GoodTillCancel"):
        if not symbol:
            logging.error("market_order request must have a request")
            return None

        payload = {
            "symbol": symbol,
            "side": side,
            "ordType": "Limit",
            "qtyType": qty_type,
            "quoteQtyEv": quote_qty_ev,
            "baseQtyEv": base_qty_ev,
            "timeInForce": time_i_force,
        }
        header = self._get_request_credentials("/spot/orders", payload, utils.get_current_timestamp())
        response = super().send_request(self, 'POST', "/spot/orders", header, data=payload)
        return response

    def get_closed_orders(self, symbol):
        if not symbol:
            logging.error("market_order request must have a request")
            return None

        params = {
            "symbol": symbol,
            "ordStatus": "Filled"
        }
        header = self._get_request_credentials("/exchange/spot/order", params, utils.get_current_timestamp())
        response = super().send_request(self, 'GET', "/exchange/spot/order", header, params)
        return response

    def get_open_orders(self, symbol):
        if not symbol:
            logging.error("market_order request must have a request")
            return None

        params = {
            "symbol": symbol
        }
        header = self._get_request_credentials("/spot/orders", params, utils.get_current_timestamp())
        response = super().send_request(self, 'GET', "/spot/orders", header, params)
        return response

    def __signed_request(self, method, url, params=None, data=None):
        header, singed_params = self._get_request_credentials(params)
        response = super().send_request(self, method, url, header, singed_params, data)
        return response

    def _get_sign(self, data):
        m = hmac.new(super().secret.encode("utf-8"), data.encode("utf-8"), hashlib.sha256)
        return m.hexdigest()

    def _get_request_credentials(self, path, params, timestamp):
        signature = self._get_sign(path + str(params) + timestamp)
        header = {"x-phemex-access-token": super().key, "x-phemex-request-signature": signature}
        return header
