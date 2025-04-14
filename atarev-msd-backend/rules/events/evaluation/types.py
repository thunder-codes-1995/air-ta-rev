from typing import Dict, Optional, TypedDict


class Pickup(TypedDict):
    ratio: int
    value: int


class EventFact(TypedDict):
    event_name: str
    country_code: str
    type: str
    sub_type: Optional[str]
    start_date: int
    end_date: int
    id: str
    city: Optional[str]


class CabinFact(TypedDict):
    cabin: str


class FlightFact(CabinFact):
    pickup: Dict[str, Pickup]


class Fct(TypedDict):
    event: EventFact
    flight: FlightFact
