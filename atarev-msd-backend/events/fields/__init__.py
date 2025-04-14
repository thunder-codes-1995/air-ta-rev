from dataclasses import dataclass
from enum import Enum
from typing import List

from base.helpers.user import User
from events.calendar.fields import hover_group
from events.common.fields import F, FieldGroup, event_name
from events.fields.form import EventFieldForm
from events.table.fields import api_groups


class Field(F):
    selected: bool


class FGroup(FieldGroup):
    fields: List[Field]


class Target(Enum):
    TABLE = "table"
    CALENDAR = "calendar"


@dataclass
class EventField:
    form: EventFieldForm
    user: User

    def get(self) -> List[FGroup]:
        if self.form.target.data == Target.TABLE.value:
            return self.__table_fields()
        return self.__calendar_fields()

    def __table_fields(self) -> List[FGroup]:
        selected = self.user.event_table_selected_fields
        if selected:
            selected = list(set(selected + ["event_name", "type", "sub_type"]))
        else:
            selected = []

        return [
            {
                "value": key,
                "label": group.label,
                "enabled": group.enabled,
                "fields": [
                    {
                        "enabled": field.enabled,
                        "label": field.label,
                        "value": field.value,
                        "selected": field.enabled and (not selected or field.value in selected),
                    }
                    for field in group.fields
                ],
            }
            for key, group in api_groups.items()
        ]

    def __calendar_fields(self) -> List[FGroup]:
        group = hover_group.json()
        return [
            {
                **group,
                "fields": [
                    {
                        "enabled": field["value"] != event_name.value,
                        "label": field["label"],
                        "value": field["value"],
                        "selected": True,
                    }
                    for field in group["fields"]
                ],
            }
        ]
