from datetime import date, datetime, timedelta
from typing import Dict, List, Literal, Tuple

from wtforms import IntegerRangeField, StringField
from wtforms.validators import NumberRange, Optional

from base.helpers.datetime import Date
from events.calendar.fields import hover_group
from events.common.form import EventForm
from events.repository import EventRepository
from utils.rules import date_range_rule, one_of

event_repo = EventRepository()


class EventCalendarForm(EventForm):
    field = StringField("field", one_of("field", ["lf", "pax", "favg", "rask"]))
    month = IntegerRangeField("month", validators=[Optional(), NumberRange(min=1, max=12)])
    fields = StringField("fields", validators=[Optional()])
    date_range_start = StringField("date_range_start", date_range_rule("date_range_start"))
    date_range_end = StringField("date_range_end", date_range_rule("date_range_end"))

    def get_date_range(self) -> Tuple[int, int]:
        s = Date(self.date_range_start.data).date()
        e = Date(self.date_range_end.data).date()
        if (e - s).days <= 365:
            return Date(self.date_range_start.data).noramlize(), Date(self.date_range_end.data).noramlize()

        today = datetime.today()
        start_date = date(year=today.year, month=today.month, day=1)
        end_date = today + timedelta(days=340)
        return (int(start_date.strftime("%Y%m%d")), int(end_date.strftime("%Y%m%d")))

    def get_field(self) -> Literal["lf", "pax", "favg", "rask"]:
        return self.field.data

    def get_labels(self) -> Dict[str, str]:
        return {f.value: f.label for f in hover_group.fields}

    def get_fields(self) -> List[str]:
        if self.list_of_string("fields"):
            return [f for f in self.list_of_string("fields") if f in hover_group.values()] or hover_group.values()
        return hover_group.values()
