from binance_spot import BinanceSpot

client = BinanceSpot(key="etC2betdZyFsHCLST5hByrFG4h1AStGvRhszilweV5iLZU68BDdyug0orYeiC42p",
                     secret="QE2U3vCuMVsE2QRtp85CJSklf4ByrayZX7kOaSkMJIPkNeChRKAV1takwtTDPN3E",
                     debug=True)

print(client.get_open_orders("BTCUSDT"))

