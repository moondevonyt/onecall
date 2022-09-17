from ftx import FTX


class FTXUS(FTX):

    def __init__(self, key=None, secret=None, **kwargs):
        if "base_url" not in kwargs:
            kwargs["base_url"] = "https://ftx.us/api"

        super().__init__(key, secret, **kwargs)
