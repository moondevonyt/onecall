from json import JSONDecodeError
import requests
import logging
import json

from .exceptions import ClientException, ServerException


class Exchange:
    """
    API Basic class
    """
    def __init__(
            self,
            key=None,
            secret=None,
            pass_phrase=None,
            base_url=None,
            show_limit_usage=False,
    ):
        """
        Initialise client

        :param key: API key
        :param secret: secret key
        :param pass_phrase: pass phrase for kucoin
        :param base_url: base url
        :param show_limit_usage: flag to show usage
        """
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
        """
        function to create and send HTTP request

        :param http_method: http method
        :param url_path: api uri path
        :param header: request header
        :param params: request query parameters
        :param data: request json data
        :return: http response
        """
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
            return self._send_error_response(e)

    def _dispatch_request(self, http_method):
        """
        function to get http dispatch method

        :param http_method: http method
        :return: http method
        """
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

    def _send_error_response(self, e: Exception):
        return {"error": e.args}
