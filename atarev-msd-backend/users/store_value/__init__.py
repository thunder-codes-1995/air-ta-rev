from dataclasses import dataclass
from typing import List

from flask import request

from base.helpers.user.actions import UserAction

from .form import EventTableFields, KPIForm


@dataclass
class StoreUserValueService:
    def store(self):
        if not request.json:
            return

        if "kpis" in request.json:
            return self.store_kpis(request.json.get("kpis"))

        if "event_table_fields" in request.json:
            return self.store_event_table_fields(request.json.get("event_table_fields"))

        # FE can send any key/value and dynamically save it to database
        else:
            key = list(request.json.keys())[0]
            value = list(request.json.values())[0]
            UserAction(request.user).add_value(key, value)
            return request.json

    def get_stored_value(self, key=None):
        if key:
            value = UserAction(request.user).get_value(key)
            return {"key": key, "value": value}
        return {}

    def store_kpis(self, kpis: List[str]) -> List[str]:
        KPIForm(fields=kpis)
        UserAction(request.user).add_value("kpis", kpis)
        return kpis

    def store_event_table_fields(self, fields: List[str]) -> List[str]:
        if not fields:
            return fields
        EventTableFields(fields=fields)
        UserAction(request.user).add_value("event_table_fields", fields)
        return fields
