from json import JSONDecodeError
import requests
import logging
import json

from .exceptions import ClientException, ServerException


class Exchange:

    def __init__(
            self,
            key=None,
            secret=None,
            pass_phrase=None,
            base_url=None,
            show_limit_usage=False,
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
        try:
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
            self._handle_exception(response)
            return response.json()
        except Exception as e:
            self._logger.error(e)
            return str(e)

    def _dispatch_request(self, http_method):
        return {
            "GET": self.session.get,
            "DELETE": self.session.delete,
            "PUT": self.session.put,
            "POST": self.session.post,
        }.get(http_method, "GET")

    def _handle_exception(self, response):
        status_code = response.status_code
        if status_code < 400:
            return
        if 400 <= status_code < 500:
            try:
                err = json.loads(response.text)
            except JSONDecodeError:
                raise ClientException(response.text)
            raise ClientException(response.text)
        raise ServerException(response.text)