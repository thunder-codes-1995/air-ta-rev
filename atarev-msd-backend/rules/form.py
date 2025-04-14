from typing import List, Optional, Tuple, Union

from pydantic import BaseModel, field_validator

from base.validators import is_carrier_code, is_ond_code, is_weekday
from rules.types import CondtionMap


def condtions_map() -> List[CondtionMap]:
    return [
        {"name": "origin", "fact": "market", "path": "market.originCityCode", "operator": "in", "field": None},
        {"name": "destination", "fact": "market", "path": "market.destCityCode", "operator": "in", "field": None},
        {"name": "competitor", "fact": "competitor", "path": "competitor.competitorCode", "operator": "equal", "field": None},
        {"name": "flight_number", "fact": "leg", "path": "leg.flightNumber", "operator": "in", "field": None},
        {"name": "weekday", "fact": "leg", "path": "leg.dayOfWeek", "operator": "in", "field": None},
        {"name": "dtd", "fact": "leg", "path": "leg.daysToDeparture", "operator": "rng", "field": "dtd"},
        {"name": "cabin", "fact": "cabin", "path": "cabin.cabinCode", "operator": "equal", "field": None},
    ]


class RuleForm(BaseModel):
    name: str
    origin: List[str]
    destination: List[str]
    competitor: str
    competitor_range: Tuple[int, int]
    cabin: str
    note: Optional[str] = None
    description: Optional[str] = None
    flight_number: Optional[List[int]] = None
    weekday: Optional[List[int]] = None
    dtd: Optional[Tuple[int, int]] = None
    id: Optional[str] = None

    @field_validator("competitor")
    def validate_competitor(cls, val: str) -> str:
        if not is_carrier_code(val):
            raise ValueError("Invalid competitor code")
        return val.upper()

    @field_validator("cabin")
    def validate_cabin(cls, val: str) -> str:
        if "eco" in val.lower():
            return "ECONOMY"

        if "bus" in val.lower():
            return "BUSINESS"

        return val.upper()

    @field_validator("weekday")
    def validate_weekday(cls, val: List[int]) -> Tuple[int, ...]:
        if not all(is_weekday(weekday) for weekday in val):
            raise ValueError("Invalid weekday")
        return tuple(val)

    @field_validator("origin")
    def validate_origin(cls, val: List[str]) -> Tuple[str, ...]:
        if not all(is_ond_code(code) for code in val):
            raise ValueError("Invalid origin value")
        return tuple(map(lambda val: val.upper(), val))

    @field_validator("destination")
    def validate_destination(cls, val: List[str]) -> Tuple[str, ...]:
        if not all(is_ond_code(code) for code in val):
            raise ValueError("Invalid destination value")
        return tuple(map(lambda val: val.upper(), val))

    @field_validator("dtd")
    def validate_dtd(cls, val: List[int]) -> Union[None, Tuple[int, int]]:
        if not val:
            return None

        if val[0] > val[1]:
            raise ValueError("Invalid dtd value")

        return (int(val[0]), int(val[1]))

    @field_validator("competitor_range")
    def validate_competitor_range(cls, val: List[int]) -> Union[None, Tuple[int, int]]:
        if val[0] > val[1]:
            raise ValueError("Invalid competitor_range value")

        return (int(val[0]), int(val[1]))
