from datetime import datetime
from enum import Enum
from typing import Collection, Dict, List, Literal, Optional, Sequence, TypedDict, Union


class Mkt(TypedDict):
    destCityCode: str
    originCityCode: str


class Lg(TypedDict):
    carrierCode: str
    flightNumber: int
    arrivalDate: int
    arrivalDay: int
    arrivalMonth: int
    arrivalTime: int
    arrivalYear: int
    dayOfWeek: int
    daysToDeparture: int
    deptDate: int
    deptDay: int
    deptMonth: int
    deptTime: int
    deptYear: int
    destCode: str
    originCode: str


class FFact(TypedDict):
    carrierCode: str
    fareAmount: float
    fareCurrency: str
    classCode: Optional[str]
    cabin: str
    deptDate: int
    deptTime: int
    lf: Union[int, None]


class CabinFact:
    cabinCode: str
    cabinCodeHumanized: str


class Fct(TypedDict):
    hostFare: Union[FFact, None]
    mainCompetitorFare: Union[FFact, None]
    lowestFare: Union[FFact, None]
    leg: Optional[Lg]
    market: Optional[Mkt]
    cabin: Optional[CabinFact]
    fares: Optional[Dict[str, float]]


class ActionParam(TypedDict):
    class_rank: str
    set_avail: int


class Action(TypedDict):
    type: str
    priority: int
    ruleID: str
    params: ActionParam


class RuleResultItem(TypedDict):
    carrier: str
    creationDate: int
    flightKey: str
    ruleId: str
    action: Action
    facts: Fct


class Rule(TypedDict):
    path: str
    fact: str
    value: Union[str, List[str], int, float, bool]
    operator: Literal["in", "equal", "greaterThanInclusive", "lessThanInclusive", "lessThan", "greaterThan"]


class SubRule(TypedDict):
    field: str
    all: Optional[List[Rule]]
    any: Optional[List[Rule]]


class Param(TypedDict):
    class_rank: Literal["CLOSEST_CLASS_1", "CLOSEST_CLASS_2", "CONTINUOUS FARE"]
    set_avail: int


class Event(TypedDict):
    type: Literal["UNDERCUT", "UPSELL", "DOWNSELL", "MATCH", "NO ACTION"]
    params: Param


class RuleEntity(TypedDict):
    ruleName: str
    rulePriority: int
    isActive: bool
    isAuto: bool
    username: str
    carrier: str
    ruleNote: Optional[str]
    ruleDescription: Optional[str]
    conditions: Dict["all", Sequence[Collection[str]]]
    effectiveDateStart: int
    effectiveDateEnd: int
    created_at: datetime
    updated_at: datetime
    event: Event


class StoreRule(TypedDict):
    name: str
    note: Optional[str]
    description: Optional[str]
    origin: List[str]
    destination: List[str]
    competitor: str
    competitor_range: List[int]
    flight_number: Optional[List[int]]
    weekday: Optional[List[int]]
    dtd: Optional[List[int]]
    id: Optional[str]
    direction: Literal["OW", "RT"]


class Action(Enum):
    CREATE = "create"
    UPDATE = "update"


class CondtionMap(TypedDict):
    name: str
    fact: str
    path: str
    operator: str
    field: Optional[str]
