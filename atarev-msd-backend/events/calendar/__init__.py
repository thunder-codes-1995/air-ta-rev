from dataclasses import dataclass
from typing import Dict, TypedDict

from base.helpers.user import User
from events.calendar.data import EventData
from events.calendar.form import EventCalendarForm
from events.calendar.type import CalendarObj


class Response(TypedDict):
    data: Dict[str, CalendarObj]
    labels: Dict[str, str]


@dataclass
class EventCalendar:
    form: EventCalendarForm
    host_code: str
    user: User

    def get(self) -> Response:
        res = {}
        event_fields = self.form.get_fields()
        events_df = EventData(self.form, self.host_code, self.user).get()
        fields = list(filter(lambda col: col not in event_fields, events_df.columns.tolist()))

        for g, g_df in events_df.groupby("date"):
            block = g_df[event_fields].drop_duplicates(event_fields).to_dict("records")
            res[g] = {**g_df[fields].iloc[0].to_dict(), "events": block}

        return {"data": res, "labels": self.form.get_labels()}
