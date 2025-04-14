import re
from datetime import datetime
from typing import Tuple, TypeVar, Union

from base.helpers.datetime import Date

T = TypeVar("T")


def is_date(val: str) -> bool:
    try:
        datetime.strptime(val, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def is_time(val: str) -> bool:
    try:
        datetime.strptime(val, "%H:%M")
        return True
    except ValueError:
        return False


def is_weekday(val: Union[str, int]) -> bool:
    if type(val) is int:
        return val >= 0 and val <= 6

    return f"{val}".lower() in ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]


def is_date_range(start_date: str, end_date: str) -> bool:
    return datetime.strptime(start_date, "%Y-%m-%d") <= datetime.strptime(end_date, "%Y-%m-%d")


def one_of(val: Union[int, bool, str, float], accept=Tuple[T, ...]) -> bool:
    return val in accept


def is_carrier_code(val: str) -> bool:
    pattern = r"^[a-zA-Z0-9]{2}$"
    return bool(re.match(pattern, val))


def is_ond_code(val: str) -> bool:
    pattern = r"^[a-zA-Z]{3}$"
    return bool(re.match(pattern, val))


def is_greater(val: Union[int, float], limit: Union[int, float]) -> bool:
    return val > limit


def is_less(val: Union[int, float], limit: Union[int, float]) -> bool:
    return val < limit


def is_greate_or_equal(val: Union[int, float], limit: Union[int, float]) -> bool:
    return val >= limit


def is_less_or_equal(val: Union[int, float], limit: Union[int, float]) -> bool:
    return val <= limit


def is_future_date(val: str) -> bool:
    return Date(val).noramlize() >= int(datetime.now().strftime("%Y%m%d"))
