from typing import List


class InvalidMarket(Exception):
    def __init__(self, origin, destination, suffix=""):
        message = f"Invalid Market, {origin} -> {destination}{suffix}"
        super().__init__(message)


class ProtectedRouter(Exception):
    def __init__(self):
        message = "Protected route, endpoint can't be accessed anonymously"
        super().__init__(message)


class UnAccessibleModulePage(Exception):
    def __init__(self, module: str, pages: List[str]):
        message = f"User does not have access to `{','.join(pages)}` pages in `{module}` module"
        super().__init__(message)


class Unauthorized(Exception):
    def __init__(self):
        message = f"user is not authorized to perform this action"
        super().__init__(message)


class ExpiredToken(Exception):
    def __init__(self):
        message = f"EXPIRED_TOKEN"
        super().__init__(message)
