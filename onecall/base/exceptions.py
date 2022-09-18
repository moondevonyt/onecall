class ServerException(Exception):
    def __init__(self, message):
        super().__init__(message)


class ClientException(Exception):
    def __init__(self, message):
        super().__init__(message)


class PandasDataframeException(Exception):
    def __init__(self, message):
        super().__init__(message)
