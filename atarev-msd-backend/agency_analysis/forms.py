from datetime import date, timedelta
from typing import Tuple, Union

from wtforms import StringField
from wtforms.validators import DataRequired

from dds.forms import BaseMSDFilterParams
from utils.rules import one_of


class AgencyTable(BaseMSDFilterParams):
    selected_yearmonth = StringField("selected_yearmonth", validators=[DataRequired()])
    time_direction = StringField("time_direction", one_of("time_direction", ["historical", "forward", "historical-forward"]))
    comp_type = StringField("comp_type", one_of("comp_type", ["month", "year"]))
    agg_type = StringField("comp_type", one_of("comp_type", ["volume", "share"]))

    def get_period(self) -> Union[Tuple[int, int, int, int], Tuple[int, None, int, None]]:
        if self.comp_type.data == "month":
            year, month = self.selected_yearmonth.data.split("-")
            year, month = int(year), int(month)
            current = date(year=year, month=month, day=1)
            prev = current - timedelta(days=30)
            return (year, month, prev.year, prev.month)

        year, month = self.selected_yearmonth.data.split("-")[0], 1
        year = int(year)
        current = date(year=year, month=month, day=1)
        prev = current - timedelta(days=30)
        return (year, None, prev.year, None)

    def growth_type(self) -> str:
        return self.agg_type.data


class AgencyQuadrant(BaseMSDFilterParams):
    selected_yearmonth = StringField("selected_yearmonth", validators=[DataRequired()])
    time_direction = StringField("time_direction", one_of("time_direction", ["historical", "forward", "historical-forward"]))
    comp_type = StringField("comp_type", one_of("comp_type", ["month", "year"]))
    selected_carrier = StringField("selected_carrier")


class AgencyGraph(BaseMSDFilterParams):
    selected_yearmonth = StringField("selected_yearmonth", validators=[DataRequired()])
    time_direction = StringField("time_direction", one_of("time_direction", ["historical", "forward", "historical-forward"]))
    comp_type = StringField("comp_type", one_of("comp_type", ["month", "year"]))
    selected_agency = StringField("selected_agency")
