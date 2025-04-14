from typing import List

from wtforms import StringField
from wtforms.validators import Optional

from base.forms import Form
from base.helpers.cabin import CabinMapper
from base.helpers.datetime import Date
from utils.rules import comma_separated_characters_only, date_range_rule


class ReportForm(Form):
    orig_city_airport = StringField("orig_city_airport", comma_separated_characters_only("orig_city_airport"))
    dest_city_airport = StringField("dest_city_airport", comma_separated_characters_only("dest_city_airport"))
    cabin = StringField("cabin", [Optional(), *comma_separated_characters_only("cabin")])
    flight = StringField("flight", [Optional(), *comma_separated_characters_only("flight")])
    outbound_date = StringField("outbound_date", [Optional(), *date_range_rule("outbound_date")])

    def get_cabin(self) -> List[list]:
        if not self.cabin.data or "all" in self.cabin.data.strip():
            return []
        return list(map(CabinMapper.normalize, self.cabin.data.split(",")))

    def get_flight_keys(self) -> List[int]:
        if not self.flight.data or "all" in self.flight.data.strip():
            return []
        return list(map(int, self.flight.data.strip().split(",")))

    def get_outbound_date(self) -> int:
        if not self.outbound_date.data:
            return
        return Date(self.outbound_date.data).noramlize()
