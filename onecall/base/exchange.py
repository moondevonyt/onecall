import requests
import logging
import datetime


class Exchange:

    def __init__(
            self,
            key=None,
            secret=None,
            pass_phrase=None,
            base_url=None,
            timeout=None,
            proxies=None,
            show_limit_usage=False,
            show_header=False,
    ):
        self.key = key
        self.secret = secret
        self.pass_phrase = pass_phrase
        self.timeout = timeout
        self.show_limit_usage = False
        self.show_header = False
        self.proxies = None
        self.session = requests.Session()

        if base_url:
            self.base_url = base_url

        if show_limit_usage is True:
            self.show_limit_usage = True

        if show_header is True:
            self.show_header = True

        if type(proxies) is dict:
            self.proxies = proxies

        self._logger = logging.getLogger(__name__)
        return

    def send_request(self, http_method, url_path, header=None, params=None, data=None):
        url = self.base_url + url_path
        self._logger.debug("url: " + url)
        payload = {
            "url": url,
            "headers": header,
            "params": params,
            "data": data
        }
        response = self._dispatch_request(http_method)(**payload)
        self._logger.debug("raw response from server:" + response.text)

        try:
            return response.json()
        except requests.exceptions.Timeout:
            logging.error("API timeout error")
        except requests.exceptions.RequestException:
            logging.error("something went wrong")
        return None

    def _dispatch_request(self, http_method):
        return {
            "GET": self.session.get,
            "DELETE": self.session.delete,
            "PUT": self.session.put,
            "POST": self.session.post,
        }.get(http_method, "GET")
