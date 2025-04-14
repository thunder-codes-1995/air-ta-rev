from typing import List, Tuple, TypedDict, Union


class Cabin(TypedDict):
    code: str
    classes: List[str]


class Flight(TypedDict):
    cabins: List[Cabin]
    bkd_lf: int
    exp_lf: int
    avg_lf: int
    flt_num: int
    flt_key: str
    arrival_date: int
    departure_date: int
    origin: str
    destination: str
    carrier_code: str
    str_departure_date: str
    str_arrival_date: str
    arrival_day: int
    arrival_month: int
    arrival_time: int
    arrival_year: int
    departure_time: int
    departure_year: int
    departure_day: int
    departure_month: int
    dtd: int
    dow: int
    comp: str
    market: str


class CompetitorCritera(TypedDict):
    code: Union[None, str]
    range: Union[None, Tuple[int, int]]


class Competitor(TypedDict):
    competitorCode: Union[str, None]
    differenceInHours: int


class EvaluationCriteria(TypedDict):
    host_code: str
    departure_date: int
    cabin: str
