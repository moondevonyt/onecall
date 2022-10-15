from onecall.binance import Binance
from onecall.binance_spot import BinanceSpot
from onecall.phemex import Phemex
from onecall.kucoin import Kucoin
from onecall.bybit import Bybit
from onecall.ftx import FTX
from onecall.ftx_us import FTXUS

exchanges = [
    "Biance",
    'Phemex',
    'Kucoin',
    'Bybit',
    'FTX',
    'Binance_spot',
]

__all__ = [
    Binance,
    BinanceSpot,
    Phemex,
    Kucoin,
    Bybit,
    FTX,
    FTXUS,
    exchanges,
]
