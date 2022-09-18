from ftx import FTX
from base import urls

class FTXUS(FTX):

    def __init__(self, key=None, secret=None, **kwargs):
        if "base_url" not in kwargs:
            kwargs["base_url"] = urls.FTXUS_FUT_BASE_URL

        super().__init__(key, secret, **kwargs)
