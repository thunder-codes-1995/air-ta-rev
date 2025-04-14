from dataclasses import dataclass

from flask import Request

from base.entities.country_city import CountryCity
from base.helpers.user import User
from events.repository import EventRepository
from rules.events.crud import CreateEventRuleForm

event_repo = EventRepository()

@dataclass
class GenerateEventRule:
    item: CreateEventRuleForm
    user: User

    def generate(self):
        return {
            "ruleName": self.item.name,
            "ruleDescription": self.item.description,
            "ruleNote": self.item.note,
            "username": self.user.username,
            "carrier": self.user.carrier,
            "conditions": {
                "all": [
                    {
                        "path": "event.country_code",
                        "operator": "equal",
                        "value": self.item.country_code,
                        "fact": "event"
                    },
                    {
                        "path": "event.id",
                        "operator": "in",
                        "value": self.item.event_id,
                        "fact": "event"
                    },
                    {
                        "path": "flight.cabin",
                        "operator": "equal",
                        "value": self.item.cabin,
                        "fact": "flight"
                    },
                    {
                        "path": f"flight.pickup.lf.{self.item.pickup_lf[0]}_{self.item.pickup_lf[1]}.value",
                        "operator": "equal",
                        "fact": "flight",
                        "value": 100
                    },
                    {
                        "field": "start_date",
                        "all": [
                            {
                                "path": "event.start_date",
                                "operator": "greaterThanInclusive",
                                "value": self.item.start_date,
                                "fact": "event"
                            },
                            {
                                "path": "event.start_date",
                                "operator": "lessThanInclusive",
                                "value": self.item.end_date,
                                "fact": "event"
                            }
                        ]
                    }
                ]
            },
            "event": {
                "type": self.item.action
            },
            "type": "E"
        }

@dataclass
class GenerateEventRuleOption:
    request: Request

    def generate(self):
        carrier = self.request.user.carrier
        cc = CountryCity(host_code=carrier).get()
        countries = set([item["country_code"] for item in cc["origins"] + cc["destinations"]])
        dte_list = [365, 270, 180, 90, 30, 15, 7, 6, 5, 4, 3, 2, 1]
        events = list(event_repo.aggregate([
          {
                        "$match": {"airline_code": carrier},
                    },
                    {
                        "$project": {
                          "_id": 0,
                          "label": "$event_name",
                          "value": {"$toString": "$_id"}
                        }}
        ]))

        days_to_event = []

        for idx, i in enumerate(dte_list):
            if idx + 1 != len(dte_list):
                days_to_event.append((dte_list[idx], dte_list[idx + 1]))

        return {
            "country_code": [{"value": c, "label": c} for c in countries],
            "days_to_event": [{"value": [i[0], i[1]], "label": f"{i[0]}-{i[1]}"} for i
                              in days_to_event],
            "cabin": [{"value": "Y", "label": "ECO"}, {"value": "J", "label": "J"}],
            "event": events,
            "function": [{"label": "LF PKUP", "value": "lf_pickup"}],
            "operators": [
                {
                    "label": "=",
                    "value": "equal",
                    "readable": "Equal"
                },
                {
                    "label": "!=",
                    "value": "notEqual",
                    "readable": "Not Equal"
                },
                {
                    "label": ">=",
                    "value": "greaterThanInclusive",
                    "readable": "Greater or Equal"
                },
                {
                    "label": ">",
                    "value": "greaterThan",
                    "readable": "Greater Then"
                },
                {
                    "label": "<=",
                    "value": "lessThanInclusive",
                    "readable": "Less or Equal"
                },
                {
                    "label": "<",
                    "value": "lessThan",
                    "readable": "Less Then"
                },
                {
                    "label": "< >",
                    "value": "between",
                    "readable": "Between"
                }
            ]
        }