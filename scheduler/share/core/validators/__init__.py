from datetime import date, datetime
from typing import Any

from utils.validator import Validator


def is_string(instance, attribute, value: str, allow_empty: bool = False) -> None:
    Validator.is_string(value, allow_empty)


def min_len(instance, attribute, value: str, limit: int) -> None:
    Validator.min_length(value, limit)


def max_len(instance, attribute, value: str, limit: int) -> None:
    Validator.max_length(value, limit)


def length(instance, attribute, value: str, limit: int) -> None:
    Validator.length(value, limit)


def is_number(instance, attribute, value: Any, min: int = None, max: int = None) -> None:
    Validator.is_number(value, min, max)


def is_reprsenting_date(val: int) -> bool:
    """check if int value represents a valid date"""
    string = f"{val}"
    if len(string) != 8:
        return False
    year, month, day = int(string[0:4]), int(string[4:6]), int(string[6:])
    d = date(year=year, month=month, day=day)
    if d.year != year or d.month != month or d.day != day:
        return False
    return True


def is_reprsenting_time(val: int) -> bool:
    """check if int value represents a valid time"""
    string = f"{val}"
    if len(string) != 6:
        return False
    h, m, s = int(string[0:2]), int(string[2:4]), int(string[4:])
    try:
        datetime(year=2023, month=10, day=10, hour=h, minute=m, second=s)
        return True
    except ValueError:
        return False
