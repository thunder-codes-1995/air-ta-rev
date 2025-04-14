from datetime import datetime
from typing import Any, Dict, Optional, TypedDict

from rules.flights.evaluation.types import CompetitorCritera, EvaluationCriteria, Flight
from rules.types import Event, Fct, RuleEntity


def competitor_criterica(rule: RuleEntity) -> CompetitorCritera:
    conditions = rule["conditions"]["all"]
    try:
        comp_obj = next(filter(lambda rule: rule.get("fact") == "mainCompetitorFare", conditions))
        rng_obj = next(filter(lambda rule: rule.get("field") == "competitor_range", conditions))
        rng = tuple(item["value"] for item in rng_obj["all"])
        return {"code": comp_obj.get("value"), "range": rng}

    except StopIteration:
        return {"code": None, "range": None}


class Action(Event):
    ruleID: str
    priority: Optional[int]


class SuccessT(TypedDict):
    carrier: str
    facts: Fct
    action: Action
    flightKey: str
    created_at: int
    ruleId: str
    identifier: str


class Criteria(TypedDict):
    carrier: str
    rule: str
    flight_key: str
    departure_date: int
    created_at: int
    cabin: str
    departure_time: int


class FailT(TypedDict):
    criteria: Criteria
    reason: Dict[str, Any]
    created_at: int


def success(carrier_code: str, cabin: str, fact: Fct, flight: Flight, rule: RuleEntity) -> SuccessT:
    _id = f"{flight['origin']}-{flight['destination']}-{flight['departure_date']}-{cabin}-{carrier_code}-{flight['flt_num']}"
    return {
        "facts": fact,
        "action": {
            **rule["event"],
            "ruleID": str(rule["_id"]),
            "ruleName": rule["ruleName"],
            "priority": rule["rulePriority"],
        },
        "flightKey": flight["flt_key"],
        "created_at": int(datetime.today().strftime("%Y%m%d%H%M%S")),
        "carrier": carrier_code,
        "ruleId": str(rule["_id"]),
        "ruleName": rule["ruleName"],
        "identifier": _id,
    }


def fail(rule: RuleEntity, flight: Flight, criteria: EvaluationCriteria, reason: Dict[str, Any]) -> FailT:
    return {
        "criteria": {
            "rule": rule["ruleName"],
            "flight_key": flight["flt_key"],
            "cabin": criteria["cabin"],
            "departure_date": criteria["departure_date"],
            "departure_time": flight["departure_time"],
            "carrier": criteria["host_code"],
        },
        "reason": reason,
        "created_at": int(datetime.today().strftime("%Y%m%d%H%M%S")),
    }
