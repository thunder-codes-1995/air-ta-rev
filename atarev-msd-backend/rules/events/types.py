from typing import Literal, Tuple, TypedDict

from rules.types import StoreRule


class Event(TypedDict):
    action = Literal["list", "email"]


class StoreEventRule(StoreRule):
    date_range: Tuple[str, str]
    direction: Literal["directional", "bi-directional"]
    event: Event
