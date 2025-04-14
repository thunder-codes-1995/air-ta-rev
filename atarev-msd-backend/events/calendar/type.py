from typing import List, Optional, TypedDict, Union


class CalendarEvent(TypedDict):
    event_name: Optional[str]
    s_date: Optional[str]
    e_date: Optional[str]
    type: Optional[str]
    sub_str: Optional[str]
    city: Optional[str]
    countries: Optional[str]


class CalendarObj(TypedDict):
    date: str
    carrier_code_host: Union[str, None]
    fare_host: Union[float, None]
    currency_host: str
    carrier_code_comp: Union[str, None]
    fare_comp: Union[float, None]
    currency_comp: str
    lf: Optional[float]
    events: List[CalendarEvent]
