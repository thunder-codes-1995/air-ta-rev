from enum import Enum
from typing import List, Literal, Tuple

from wtforms import BooleanField, StringField
from wtforms.validators import Optional

from base.forms import Form
from base.helpers.datetime import Date
from utils.rules import date_range_rule, one_of


class AggType(Enum):
    MONTHLY = "monthly"
    DAILY = "daily"


class BookingTrendsForm(Form):
    graph_carriers = StringField("graph_carriers", [Optional()])
    date_range_start = StringField("date_range_start", date_range_rule("date_range_start"))
    date_range_end = StringField("date_range_end", date_range_rule("date_range_end"))
    show_holidays = BooleanField("show_holidays", [Optional()])
    agg_type = StringField("agg_type", one_of("agg_type", [AggType.MONTHLY.value, AggType.DAILY.value]))
    holiday_countries = StringField("holiday_countries", [Optional()])
    selected_holidays = StringField("selected_holidays", [Optional()])
    orig_city_airport = StringField("orig_city_airport")
    dest_city_airport = StringField("dest_city_airport")
    pos = StringField("pos", [Optional()])
    main_competitor = StringField("main_competitor")
    sales_channel = StringField("sales_channel", [Optional()])
    cabin = StringField("cabin", [Optional()])
    time_direction = StringField("time_direction", one_of("time_direction", ["historical", "forward", "historical-forward"]))

    def get_carriers(self) -> List[str]:
        return self.list_of_string("graph_carriers")

    def get_date_range(self) -> Tuple[int, int]:
        start = Date(self.date_range_start.data).noramlize()
        end = Date(self.date_range_end.data).noramlize()
        return (start, end)

    def get_show_holidays(self) -> bool:
        if not self.show_holidays.data:
            return True
        return self.show_holidays.data

    def get_holiday_countries(self) -> List[str]:
        return self.list_of_string("holiday_countries")

    def get_selected_holidays(self) -> List[str]:
        return self.list_of_string("selected_holidays")

    def get_pos(self) -> List[str]:
        return self.list_of_string("pos")

    def get_sales_channel(self) -> List[str]:
        return self.list_of_string("sales_channel")

    def get_agg_type(self) -> Literal["monthly", "daily"]:
        return self.agg_type.data
