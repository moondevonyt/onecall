from onecall.ftx import FTX
from onecall.base import urls


class FTXUS(FTX):
    """
    API class for FTX.US
    """
    def __init__(self, key=None, secret=None, **kwargs):
        """
        FTX.US API class
        https://docs.ftx.com/#rest-api

        :param key: API key
        :param secret: Secret key
        """
        kwargs["base_url"] = urls.FTXUS_FUT_BASE_URL

        super().__init__(key, secret, **kwargs)
        return

    def get_positions(self, symbol):
        return {"error": "spot market don't have position concept. check balance instead"}
