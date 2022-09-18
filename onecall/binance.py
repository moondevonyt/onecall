import datetime
import logging
import pandas as pd

from base import utils
from base.exchange import Exchange
from binance import um_futures, cm_futures


class Binance(Exchange):

    def __init__(self, key=None, secret=None, future_type=None):
        # For coin future
        if future_type == "COIN":
            self._client = cm_futures.CMFutures(key=key, secret=secret)
        else:
            # For USD future
            self._client = um_futures.UMFutures(key=key, secret=secret)

    def get_positions(self, symbol: str):
        """
        API to get current positions
        :param symbol: future symbol
        :return:[
            {
                "entryPrice": "0.00000",
                "marginType": "isolated",
                "isAutoAddMargin": "false",
                "isolatedMargin": "0.00000000",
                "leverage": "10",
                "liquidationPrice": "0",
                "markPrice": "6679.50671178",
                "maxNotionalValue": "20000000",
                "positionAmt": "0.000",
                "notional": "0",,
                "isolatedWallet": "0",
                "symbol": "BTCUSDT",
                "unRealizedProfit": "0.00000000",
                "positionSide": "BOTH",
                "updateTime": 0
            }
        ]
        """
        return self._client.get_position_risk(symbol=symbol)

    def cancel_orders(self, symbol: str):
        """
        API to cancel all the active orders
        :param symbol: future symbol
        :return: [
            {
                "clientOrderId": "myOrder1",
                "cumQty": "0",
                "cumQuote": "0",
                "executedQty": "0",
                "orderId": 283194212,
                "origQty": "11",
                "origType": "TRAILING_STOP_MARKET",
                "price": "0",
                "reduceOnly": false,
                "side": "BUY",
                "positionSide": "SHORT",
                "status": "CANCELED",
                "stopPrice": "9300",                // please ignore when order type is TRAILING_STOP_MARKET
                "closePosition": false,   // if Close-All
                "symbol": "BTCUSDT",
                "timeInForce": "GTC",
                "type": "TRAILING_STOP_MARKET",
                "activatePrice": "9020",            // activation price, only return with TRAILING_STOP_MARKET order
                "priceRate": "0.3",                 // callback rate, only return with TRAILING_STOP_MARKET order
                "updateTime": 1571110484038,
                "workingType": "CONTRACT_PRICE",
                "priceProtect": false            // if conditional order trigger is protected
            },
            {
                "code": -2011,
                "msg": "Unknown order sent."
            }
        ]
        """
        return self._client.cancel_batch_order(symbol=symbol)

    def get_data(self, symbol: str, interval: int, st_date: datetime.datetime = None, ed_date: datetime.datetime = None,
                 limit=500, is_dataframe=False):
        """
        API to get OHLCV data
        :param symbol: future symbol
        :param interval: time interval
        :param st_date: start time of the data
        :param ed_date: end time of the data
        :param limit: number of data limit
        :param is_dataframe: convert the data to pandas dataframe
        :return: list of list/ pandas dataframe
        """
        params = {
            "symbol": symbol,
            "interval": interval,
            "limit": limit,
        }
        if st_date:
            params["startTime"] = st_date

        if ed_date:
            params["endTime"] = ed_date
        response = self._client.klines(**params)
        if is_dataframe:
            try:
                columns = ['Open Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close time',
                           'Quote asset volume', 'Number of trades', 'Taker buy base asset volume',
                           'Taker buy quote asset volume', 'ignore']
                return pd.DataFrame(response, columns=columns)
            except :
                logging.error("failed to create dataframe")
        return response

    def get_orderbook(self, symbol: str, limit=500, is_dataframe=False):
        """
        Get orderbook
        :param symbol: future symbol
        :param limit: result limit; default 500
        :param is_dataframe: convert the data to pandas dataframe
        :return: {
              "lastUpdateId": 1027024,
              "E": 1589436922972,   // Message output time
              "T": 1589436922959,   // Transaction time
              "bids": [
                [
                  "4.00000000",     // PRICE
                  "431.00000000"    // QTY
                ]
              ],
              "asks": [
                [
                  "4.00000200",
                  "12.00000000"
                ]
              ]
            }
        """
        params = {
            "symbol": symbol,
            "limit": limit
        }
        response = self._client.depth(**params)
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
        :return: [
                    {
                        "accountAlias": "SgsR",    // unique account code
                        "asset": "USDT",    // asset name
                        "balance": "122607.35137903", // wallet balance
                        "crossWalletBalance": "23.72469206", // crossed wallet balance
                        "crossUnPnl": "0.00000000"  // unrealized profit of crossed positions
                        "availableBalance": "23.72469206",       // available balance
                        "maxWithdrawAmount": "23.72469206",     // maximum amount for transfer out
                        "marginAvailable": true,    // whether the asset can be used as margin in Multi-Assets mode
                        "updateTime": 1617939110373
                    }
                ]
        """
        return self._client.balance()

    def market_order(self, symbol: str, side: str, quantity: int):
        """
        API to place market order
        :param symbol: future symbol
        :param side: buy/sell
        :param quantity:
        :return: {
            "clientOrderId": "testOrder",
            "cumQty": "0",
            "cumQuote": "0",
            "executedQty": "0",
            "orderId": 22542179,
            "avgPrice": "0.00000",
            "origQty": "10",
            "price": "0",
            "reduceOnly": false,
            "side": "BUY",
            "positionSide": "SHORT",
            "status": "NEW",
            "stopPrice": "9300",        // please ignore when order type is TRAILING_STOP_MARKET
            "closePosition": false,   // if Close-All
            "symbol": "BTCUSDT",
            "timeInForce": "GTC",
            "type": "TRAILING_STOP_MARKET",
            "origType": "TRAILING_STOP_MARKET",
            "activatePrice": "9020",    // activation price, only return with TRAILING_STOP_MARKET order
            "priceRate": "0.3",         // callback rate, only return with TRAILING_STOP_MARKET order
            "updateTime": 1566818724722,
            "workingType": "CONTRACT_PRICE",
            "priceProtect": false            // if conditional order trigger is protected
        }
        """
        params = {
            "symbol": symbol,
            "side": side.upper(),
            "type": "MARKET",
            "quantity": quantity,
        }
        return self._client.new_order(**params)

    def limit_order(self, symbol: str, side: str, quantity: int, price: float, time_in_force="GTC"):
        """
        API to place limit order
        :param symbol: future symbol
        :param side: buy/sell
        :param quantity: trade quantity
        :param price: trading price
        :param time_in_force: for postOnly request
        :return: {
            "clientOrderId": "testOrder",
            "cumQty": "0",
            "cumQuote": "0",
            "executedQty": "0",
            "orderId": 22542179,
            "avgPrice": "0.00000",
            "origQty": "10",
            "price": "0",
            "reduceOnly": false,
            "side": "BUY",
            "positionSide": "SHORT",
            "status": "NEW",
            "stopPrice": "9300",        // please ignore when order type is TRAILING_STOP_MARKET
            "closePosition": false,   // if Close-All
            "symbol": "BTCUSDT",
            "timeInForce": "GTC",
            "type": "TRAILING_STOP_MARKET",
            "origType": "TRAILING_STOP_MARKET",
            "activatePrice": "9020",    // activation price, only return with TRAILING_STOP_MARKET order
            "priceRate": "0.3",         // callback rate, only return with TRAILING_STOP_MARKET order
            "updateTime": 1566818724722,
            "workingType": "CONTRACT_PRICE",
            "priceProtect": false            // if conditional order trigger is protected
        }
        """
        params = {
            "symbol": symbol,
            "side": side.upper(),
            "type": "LIMIT",
            "quantity": quantity,
            "price": price,
            "timeInForce": time_in_force,
            "timestamp": utils.get_current_timestamp()
        }
        return self._client.new_order(**params)

    def get_closed_orders(self, symbol: str):
        """
        API to get all the closed orders
        :param symbol: future symbol
        :return: [
              {
                "avgPrice": "0.00000",
                "clientOrderId": "abc",
                "cumQuote": "0",
                "executedQty": "0",
                "orderId": 1917641,
                "origQty": "0.40",
                "origType": "TRAILING_STOP_MARKET",
                "price": "0",
                "reduceOnly": false,
                "side": "BUY",
                "positionSide": "SHORT",
                "status": "NEW",
                "stopPrice": "9300",                // please ignore when order type is TRAILING_STOP_MARKET
                "closePosition": false,   // if Close-All
                "symbol": "BTCUSDT",
                "time": 1579276756075,              // order time
                "timeInForce": "GTC",
                "type": "TRAILING_STOP_MARKET",
                "activatePrice": "9020",            // activation price, only return with TRAILING_STOP_MARKET order
                "priceRate": "0.3",                 // callback rate, only return with TRAILING_STOP_MARKET order
                "updateTime": 1579276756075,        // update time
                "workingType": "CONTRACT_PRICE",
                "priceProtect": false            // if conditional order trigger is protected
              }
            ]
        """
        params = {
            "symbol": symbol
        }
        response = self._client.get_all_orders(**params)
        return list(filter(lambda order: order["status"] == "FILLED", response))

    def get_open_orders(self, symbol: str):
        """
        API to get all active orders
        :param symbol: symbol
        :return: [
              {
                "avgPrice": "0.00000",
                "clientOrderId": "abc",
                "cumQuote": "0",
                "executedQty": "0",
                "orderId": 1917641,
                "origQty": "0.40",
                "origType": "TRAILING_STOP_MARKET",
                "price": "0",
                "reduceOnly": false,
                "side": "BUY",
                "positionSide": "SHORT",
                "status": "NEW",
                "stopPrice": "9300",                // please ignore when order type is TRAILING_STOP_MARKET
                "closePosition": false,   // if Close-All
                "symbol": "BTCUSDT",
                "time": 1579276756075,              // order time
                "timeInForce": "GTC",
                "type": "TRAILING_STOP_MARKET",
                "activatePrice": "9020",            // activation price, only return with TRAILING_STOP_MARKET order
                "priceRate": "0.3",                 // callback rate, only return with TRAILING_STOP_MARKET order
                "updateTime": 1579276756075,        // update time
                "workingType": "CONTRACT_PRICE",
                "priceProtect": false            // if conditional order trigger is protected
              }
            ]
        """
        params = {
            "symbol": symbol
        }
        return self._client.get_open_orders(**params)
