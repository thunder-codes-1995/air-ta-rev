from typing import Any, Dict, Optional

from base.helpers.fields import Field
from base.repository import BaseRepository


class RuleRepository(BaseRepository):
    collection = "bre_rules"

    def get(self, match: Optional[Dict[str, Any]] = None):
        return self.find({**(match or {})}).sort("rulePriority", 1)


class RuleResultRepository(BaseRepository):
    collection = "bre_rules_results"

    def get(self, match={}, flatten: bool = False):
        if not flatten:
            return self.aggregate(
                [
                    {"$match": {**match, "type": "E"}},
                    {
                        "$project": {
                            "_id": 0,
                            "id": {"$toString": "$_id"},
                            "carrier": 1,
                            "rule_id": "$ruleId",
                            "name": "$ruleName",
                            "event": 1,
                            "flight": 1,
                            "action": 1,
                        }
                    },
                ]
            )

        return self.aggregate(
            [
                {"$match": {**match, "type": "E"}},
                {
                    "$project": {
                        "_id": 0,
                        "id": {"$toString": "$_id"},
                        "carrier": 1,
                        "action": 1,
                        "rule_id": "$ruleId",
                        "created_at": 1,
                        "rule_name": "$ruleName",
                        "event_name": "$event.event_name",
                        "country_code": "$event.country_code",
                        "type": "$event.type",
                        "sub_type": "$event.type",
                        "start_date": "$event.start_date",
                        "end_date": "$event.end_date",
                        "city": "$event.city",
                        **Field.date_as_string("str_start_date", "event.start_date"),
                        **Field.date_as_string("str_end_date", "event.end_date"),
                        "cabin": "$flight.cabin",
                        "lf_pickup_ratio": "$flight.pickup.lf.ratio",
                        "lf_pickup_value": "$flight.pickup.lf.value",
                        "lf_pickup_range": {
                            "$concat": [
                                {"$toString": {"$first": "$flight.pickup.lf.range"}},
                                "-",
                                {"$toString": {"$last": "$flight.pickup.lf.range"}},
                            ]
                        },
                    }
                },
            ]
        )


class RuleFailResultRepository(BaseRepository):
    collection = "bre_rules_fail"
