from ftx import FTX
from base import urls


class FTXUS(FTX):
    """
    API class for FTXUS
    """
    def __init__(self, key=None, secret=None, debug=False, **kwargs):
        if not debug:
            kwargs["base_url"] = urls.FTXUS_FUT_BASE_URL
        else:
            kwargs["base_url"] = urls.FTXUS_FUT_TEST_BASE_URL

        super().__init__(key, secret, **kwargs)
        return
