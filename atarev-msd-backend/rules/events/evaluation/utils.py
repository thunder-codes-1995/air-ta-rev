from datetime import datetime
from typing import Optional, Tuple, TypedDict

from rules.events.evaluation.types import EventFact, Fct, FlightFact
from rules.types import RuleEntity


class EventT(TypedDict):
    action: str


class PickupT(TypedDict):
    ratio: int
    value: int
    range: Tuple[int, int]


class FlightT(FlightFact):
    pickup: PickupT


class SuccessT(TypedDict):
    event: EventFact
    flight: FlightT
    type: str
    created_at: int
    carrier: str
    ruleId: str
    ruleName: str
    action: Optional[EventT]


def extract_pickup_range(rule: RuleEntity) -> Tuple[int, int]:
    conditions = rule["conditions"]["all"]
    obj = next(filter(lambda rule: "flight.pickup" in rule.get("path"), conditions))
    value = obj["path"].split(".")[2]
    start, end = list(map(int, value.split("_")))
    return start, end


def success(carrier_code: str, fact: Fct, rule: RuleEntity) -> SuccessT:
    st, en = extract_pickup_range(rule)
    obj = {
        "type": "E",
        "created_at": int(datetime.today().strftime("%Y%m%d%H%M%S")),
        "carrier": carrier_code,
        "ruleId": str(rule["_id"]),
        "ruleName": rule["ruleName"],
        "event": fact["event"],
        "flight": {
            "cabin": fact["flight"]["cabin"],
            "pickup": {
                "ratio": fact["flight"]["pickup"][f"{st}_{en}"]["ratio"],
                "value": fact["flight"]["pickup"][f"{st}_{en}"]["value"],
                "range": (st, en),
            },
        },
    }

    if rule.get("event"):
        obj["action"] = rule["event"]["type"]

    return obj
