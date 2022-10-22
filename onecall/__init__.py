from onecall.base.exchange import Exchange

from onecall.binance import Binance as binance
from onecall.binance_spot import BinanceSpot as binancespot
from onecall.phemex import Phemex as phemex
from onecall.kucoin import Kucoin as kucoin
from onecall.bybit import Bybit as bybit
from onecall.ftx_us import FTXUS as ftxus
from onecall.ftx import FTX as ftx

exchanges = [
    "Biance",
    'Phemex',
    'Kucoin',
    'Bybit',
    'FTX',
    'Binance_spot',
]

__all__ = [
    binance,
    binancespot,
    phemex,
    kucoin,
    bybit,
    ftxus,
    ftx,
    exchanges,
]
