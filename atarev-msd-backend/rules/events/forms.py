from typing import List, Literal, Tuple

from pydantic import BaseModel, field_validator

from base.helpers.datetime import Date
from base.helpers.user import User
from base.validators import is_date_range
from rules.build import conditions
from rules.form import RuleForm
from rules.form import condtions_map as rule_conditions_map
from rules.types import CondtionMap


def condtions_map() -> List[CondtionMap]:
    return [
        *rule_conditions_map(),
        {"name": "direction", "fact": "leg", "path": "leg.direction", "operator": "equal", "field": None},
        # {"name": "date_range", "fact": "leg", "path": "leg.deptDate", "operator": "rng", "field": "date_range"},
    ]


class Event(BaseModel):
    action: Literal["list", "email"]


class EventRuleForm(RuleForm):
    date_range: Tuple[str, str]
    direction: Literal["OW", "RW"]
    event: Event

    @field_validator("date_range")
    def validate_date_range(cls, val: List[str]) -> Tuple[int, int]:
        if not is_date_range(start_date=val[0], end_date=val[1]):
            raise ValueError("Invalid date range")
        return (Date(val[0]).noramlize(), Date(val[1]).noramlize())

    def data(self, user: User):

        return {
            "ruleName": self.name,
            "ruleNote": self.note,
            "ruleDescription": self.description,
            "carrier": user.carrier,
            "date_range_start": self.date_range[0],
            "date_range_end": self.date_range[1],
            "target": "events",
            "event": {"type": self.event.action},
            "conditions": conditions(
                {
                    "dtd": self.dtd,
                    "origin": self.origin,
                    "destination": self.destination,
                    "competitor": self.competitor,
                    "flight_number": self.flight_number,
                    "weekday": self.weekday,
                    "weekday": self.weekday,
                    "cabin": self.cabin,
                    "direction": self.direction,
                    # "date_range": self.date_range,
                },
                condtions_map(),
            ),
        }
