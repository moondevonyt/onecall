import requests
import logging


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
        self.show_limit_usage = False
        self.session = requests.Session()

        if base_url:
            self.base_url = base_url

        if show_limit_usage is True:
            self.show_limit_usage = True

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
            self._logger.error(requests.exceptions.Timeout)
        except requests.exceptions.RequestException:
            self._logger.error(requests.exceptions.RequestException)
        return response.text

    def _dispatch_request(self, http_method):
        return {
            "GET": self.session.get,
            "DELETE": self.session.delete,
            "PUT": self.session.put,
            "POST": self.session.post,
        }.get(http_method, "GET")
