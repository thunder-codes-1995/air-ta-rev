from datetime import datetime
from typing import Optional, List

from pydantic import field_validator, BaseModel

from base.validators import is_weekday
from rules.form import RuleForm

class AnalysisItem(BaseModel):
    apply: str
    operator: str
    value: List[int]



class EventItem(BaseModel):
    class EventParamsItem(BaseModel):
        class_rank: str
        set_avail: int
    action: str
    params: EventParamsItem

class CreateFlightRuleForm(RuleForm):
    priority: Optional[int] = None
    is_active: Optional[bool] = None
    is_auto: Optional[bool] = None
    effective_time : Optional[List[str]] = None
    departure_date: Optional[List[str]] = None
    effective_date: Optional[List[str]] = None
    equipment: Optional[List[str]] = None
    dow: Optional[List[int]] = None
    analysis: Optional[List[AnalysisItem]] = None
    event: Optional[EventItem] = None
    id: Optional[str] = None

    @field_validator("dow")
    def validate_dow(cls, val: List[int]) -> List[int]:
        if not all(is_weekday(weekday) for weekday in val):
            raise ValueError("Invalid dow")
        return val

    @field_validator("effective_time")
    def validate_effective_time(cls, val: str) -> str:
        first, second = val

        try:
            first_dt = datetime.strptime(first, "%H:%M")
            second_dt = datetime.strptime(second, "%H:%M")
        except ValueError:
            raise ValueError("Invalid time format")

        if second_dt <= first_dt:
            raise ValueError("Second time, should greater then first time")

        return val

    @classmethod
    def _validate_datetime(cls, val):
        first, second = val
        try:
            first_dt = datetime.strptime(first, "%Y-%m-%d")
            second_dt = datetime.strptime(second, "%Y-%m-%d")
        except ValueError:
            raise ValueError("Invalid date format")
        if second_dt <= first_dt:
            raise ValueError("Second date, should greater then first date")
        return [first_dt.strftime("%Y%m%d"), second_dt.strftime("%Y%m%d")]

    @field_validator("departure_date")
    def validate_departure_date(cls, val):
        return cls._validate_datetime(val)

    @field_validator("effective_date")
    def validate_effective_date(cls, val):
        return cls._validate_datetime(val)
