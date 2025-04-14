from typing import TypedDict


class AvTrendsTableObj(TypedDict):
    maf: str
    departure_date: str
    weekday: str
    type: str
    carrierCode: str
    fltNum: int
    inFltNum: int
    deptTime: str
    lf: str
    duration: str
    is_connecting: bool
    cabinName: str
    classCode: str
