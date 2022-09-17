import hmac
import hashlib
import logging
import pandas as pd

from base import utils
from base.exchange import Exchange


class Bybit(Exchange):

    def __init__(self, key=None, secret=None, **kwargs):
        self.recv_window = "5000"
        if "base_url" not in kwargs:
            kwargs["base_url"] = "https://api.bybit.com"

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
            "api_key": super().key,
            "symbol": symbol,
            "timestamp": utils.get_current_timestamp()
        }
        query_string = self.__get_query_string(params)
        header = self._get_request_credentials(query_string, params["timestamp"])
        params["sign"] = header["X-BAPI-SIGN"]
        response = super().send_request(self, 'GET', "/private/linear/position/list", header, params)
        return response

    def cancel_orders(self, symbol):
        """
        API to cancel all orders
        :param symbol: symbol
        :return:
        """
        if not symbol:
            logging.error("get_position request must have a request")
            return None

        payload = {
            "api_key": super().key,
            "symbol": symbol,
            "timestamp": utils.get_current_timestamp()
        }
        payload_string = self.__get_payload_string(payload)
        header = self._get_request_credentials(payload_string, payload["timestamp"])
        payload["sign"] = header["X-BAPI-SIGN"]
        active_response = super().send_request(self, 'DELETE', "/private/linear/order/cancel-all", header, data=payload)
        conditional_response = super().send_request(self, 'DELETE', "/private/linear/stop-order/cancel-all", header,
                                                    data=payload)
        return {"active_order": active_response, "conditional_order": conditional_response}

    def get_data(self, symbol, interval, start_time, limit=500, is_dataframe=False):
        """
        API to get OHLCV data
        :param symbol:
        :param interval:
        :param start_time:
        :param limit:
        :param is_dataframe:
        :return:
        """
        if not symbol:
            logging.error("get_position request must have a request")
            return None

        params = {
            "symbol": symbol,
            "interval": interval,
            "limit": limit,
            "from": start_time
        }
        response = super().send_request(self, 'GET', "/public/linear/kline", params=params)
        if is_dataframe:
            try:
                return pd.DataFrame(response["result"])
            except :
                logging.error("failed to create dataframe")
        return response

    def get_orderbook(self, symbol, is_dataframe=False):
        if not symbol:
            logging.error("get_position request must have a request")
            return None

        params = {
            "symbol": symbol
        }
        response = super().send_request(self, 'GET', "/v2/public/orderBook/L2", params=params)
        if is_dataframe:
            try:
                return pd.DataFrame(response["result"])
            except :
                logging.error("failed to create dataframe")
        return response

    def get_balance(self):
        param = {
            "api_key": super().key,
            "timestamp": utils.get_current_timestamp()
        }
        param_string = self.__get_query_string(param)
        header = self._get_request_credentials(param_string, param["timestamp"])
        param["sign"] = header["X-BAPI-SIGN"]
        response = super().send_request(self, 'GET', "/v2/private/wallet/balance", header, param)
        return response

    def market_order(self, symbol, side, quantity):
        if not symbol:
            logging.error("get_position request must have a request")
            return None

        payload = {
            "api_key": super().key,
            "side": side,
            "symbol": symbol,
            "order_type": "Market",
            "qty": quantity,
            "timestamp": utils.get_current_timestamp()
        }
        payload_string = self.__get_payload_string(payload)
        header = self._get_request_credentials(payload_string, payload["timestamp"])
        payload["sign"] = header["X-BAPI-SIGN"]
        response = super().send_request(self, 'POST', "/private/linear/order/create", header, data=payload)
        return response

    def limit_order(self, symbol, side, quantity, price, time_in_force="GTC"):
        if not symbol:
            logging.error("limit_order request must have a request")
            return None

        payload = {
            "api_key": super().key,
            "side": side,
            "symbol": symbol,
            "order_type": "Limit",
            "qty": quantity,
            "price": price,
            "timestamp": utils.get_current_timestamp()
        }
        payload_string = self.__get_payload_string(payload)
        header = self._get_request_credentials(payload_string, payload["timestamp"])
        payload["sign"] = header["X-BAPI-SIGN"]
        response = super().send_request(self, 'POST', "/private/linear/order/create", header, data=payload)
        return response

    def get_closed_orders(self, symbol):
        if not symbol:
            logging.error("get_closed_orders request must have a request")
            return None

        param = {
            "api_key": super().key,
            "symbol": symbol,
            "timestamp": utils.get_current_timestamp()
        }
        param_string = self.__get_query_string(param)
        header = self._get_request_credentials(param_string, param["timestamp"])
        param["sign"] = header["X-BAPI-SIGN"]
        response = super().send_request(self, 'GET', "/private/linear/order/search", header, param)
        return list(map(lambda order: order["order_status"] == "Filled", response.get("result", [])))

    def get_open_orders(self, symbol):
        if not symbol:
            logging.error("get_closed_orders request must have a request")
            return None

        param = {
            "api_key": super().key,
            "symbol": symbol,
            "timestamp": utils.get_current_timestamp()
        }
        param_string = self.__get_query_string(param)
        header = self._get_request_credentials(param_string, param["timestamp"])
        param["sign"] = header["X-BAPI-SIGN"]
        response = super().send_request(self, 'GET', "/private/linear/order/search", header, param)
        return list(map(lambda order: order["order_status"] == "New", response.get("result", [])))

    def __signed_request(self, method, url, params=None, data=None):
        header, singed_params = self._get_request_credentials(params)
        response = super().send_request(self, method, url, header, singed_params, data)
        return response

    def _get_sign(self, payload, timestamp):
        param_str = str(timestamp) + super().key + self.recv_window + payload
        sign = hmac.new(bytes(super().secret, "utf-8"), param_str.encode("utf-8"), hashlib.sha256)
        return sign.hexdigest()

    def _get_request_credentials(self, params_string, timestamp):
        sign = self._get_sign(params_string, timestamp)
        headers = {
            'X-BAPI-API-KEY': super().key,
            'X-BAPI-SIGN': sign,
            'X-BAPI-SIGN-TYPE': '2',
            'X-BAPI-TIMESTAMP': timestamp,
            'X-BAPI-RECV-WINDOW': self.recv_window,
            'Content-Type': 'application/json'
        }
        return headers

    def __get_query_string(self, params):
        return "&".join(list(map(lambda kv: kv[0] + "=" + str(kv[1]), params.items())))

    def __get_payload_string(self, params):
        return "{"+",".join(list(map(lambda kv: kv[0] + "=" + str(kv[1]), params.items()))) + "}"
