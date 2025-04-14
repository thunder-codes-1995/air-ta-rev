from collections.abc import Iterable
from typing import Any


class Validator:

    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super().__new__(cls)
        return cls.instance

    @classmethod
    def is_string(cls, value: str, allow_empty: bool = False) -> None:
        assert type(value) == str
        if not allow_empty:
            assert bool(value.strip())

    @classmethod
    def min_length(cls, value: str, limit: int) -> None:
        assert limit >= 0
        assert len(value) >= limit

    @classmethod
    def max_length(cls, value: str, limit: int) -> None:
        assert limit >= 0
        assert len(value) <= limit

    @classmethod
    def length(cls, value: str, limit: int) -> None:
        assert limit >= 0
        assert len(value) == limit

    @classmethod
    def is_in(cls, value: Any, search: Iterable) -> None:
        assert isinstance(search, Iterable)
        assert value in search

    @classmethod
    def is_number(cls, value: Any, _min=None, _max=None) -> None:
        assert f"{abs(value)}".isnumeric()
        if _min:
            assert int(value) >= _min
        if _max:
            assert int(value) <= _max
