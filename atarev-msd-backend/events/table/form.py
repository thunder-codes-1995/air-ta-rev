import calendar
from datetime import datetime
from typing import Dict, List, Tuple

from wtforms import IntegerField, IntegerRangeField, StringField
from wtforms.validators import DataRequired, NumberRange, Optional

from airports.repository import AirportRepository
from base.entities.carrier import CityBasedMarket
from events.common.form import EventForm
from events.table.fields import API, labels

airport_repo = AirportRepository()


class EventTableForm(EventForm):
    fields = StringField("fields", [Optional()])
    year = IntegerField("year", [DataRequired()])
    month = IntegerRangeField("month", validators=[DataRequired(), NumberRange(min=1, max=12)])

    def get_main_competitor(self, host_code: str) -> str:
        origin = self.get_origin()[0]
        destination = self.get_destination()[0]
        comps = CityBasedMarket(host_code, origin, destination).competitors()
        assert bool(comps), "Invalid Market"
        return comps[1]

    def get_selected_fields(self) -> List[str]:
        if not self.fields.data:
            return []
        _all = API.all()
        return [f for f in self.fields.data.strip().split(",") if f in _all]

    def get_days_count(self) -> int:
        curreny_year = datetime.now().year
        return calendar.monthrange(curreny_year, self.month.data)[1]

    def get_years_range(self) -> Tuple[int, int]:
        curreny_year = datetime.now().year

        if self.year.data == curreny_year:
            return (curreny_year - 1, curreny_year)

        if self.year.data < curreny_year:
            return (self.year.data, curreny_year)

        return (curreny_year, self.year.data)

    def labels(self) -> Dict[str, str]:
        res = {}
        selected = self.get_selected_fields()

        for f in labels:

            if not f.enabled:
                continue

            if selected and f.value not in selected:
                continue

            res[f.value] = f.label

        return res
