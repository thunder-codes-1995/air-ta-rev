from typing import Any


class ValidationError(Exception):
    def __init__(self, field: str = '', value: Any = None, message=''):
        msg = f"validation Error, field:\"{field}\" - value:\"{value}\" - original error message:\"{message}\""
        super().__init__(msg)


class InvalidParameterException(ValueError):
    def __init__(self, message: str):
        super().__init__(message)


class MissingEnvironmentPropertyException(KeyError):
    def __init__(self, env_key: str):
        super().__init__(f"Missing environment property: {env_key}")


class MissingInputFileException(IOError):
    def __init__(self, filename: str):
        super().__init__(f"Missing input file: {filename}")


class HititIntervalException(Exception):
    def __init__(self, message: str):
        super().__init__(message)
