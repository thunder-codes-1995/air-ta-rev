from wtforms import StringField
from wtforms.validators import DataRequired, Optional

from dds.forms import BaseMSDFilterParams
from utils.rules import comma_separated_characters_only, date_range_rule, one_of


class BookingTrends(BaseMSDFilterParams):
    time_direction = StringField("time_direction", one_of("time_direction", ["historical", "forward", "historical-forward"]))
    date_range_start = StringField("date_range_start", date_range_rule("date_range_start"))
    date_range_end = StringField("date_range_end", date_range_rule("date_range_end"))
    agg_type = StringField("agg_type", validators=one_of("time_direction", ["agg_type", "monthly", "daily"]))
    graph_carriers = StringField("selected_competitors", comma_separated_characters_only("graph_carriers"))
    holiday_countries = StringField("holiday_countries", [Optional(), *comma_separated_characters_only("holiday_countries")])
    show_holidays = StringField("show_holidays", validators=[DataRequired()])
    selected_holidays = StringField("selected_holidays", validators=[DataRequired()])


class BookingCurve(BaseMSDFilterParams):
    time_direction = StringField("time_direction", one_of("time_direction", ["historical", "forward", "historical-forward"]))
    agg_type = StringField("agg_type", validators=[DataRequired()])
    val_type = StringField("val_type", validators=one_of("val_type", ["ratio", "total-bookings"]))
    graph_carriers = StringField("graph_carriers", [*comma_separated_characters_only("graph_carriers"), Optional()])
    dtd = StringField("dtd", validators=[DataRequired()])
    # for some value frontend is sending year_month as "YYYY-MM-DD" istead of "YYYY-MM"
    selected_yearmonth = StringField("selected_yearmonth", validators=[DataRequired()])


class BookingCountryOptions(BaseMSDFilterParams):
    graph_carriers = StringField("graph_carriers", [*comma_separated_characters_only("graph_carriers"), Optional()])
