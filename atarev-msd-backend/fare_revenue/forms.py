from dds.forms import BaseMSDFilterParams
from utils.rules import comma_separated_characters_only, date_range_rule, one_of


from wtforms import StringField
from wtforms.validators import Optional


class MSDProductRevenueFareTrends(BaseMSDFilterParams):
    time_direction = StringField('time_direction',
                                 one_of('time_direction', ["historical", "forward", "historical-forward"]))
    graph_carriers = StringField("selected_competitors",
                                 [*comma_separated_characters_only("graph_carriers"), Optional()])


class MSDBookingVsAverageFares(BaseMSDFilterParams):
    view_opt = StringField('view_opt', one_of(
        'time_direction', ["cumulative", "individual"]))
    # override selected_competitors because it is required in parent class
    selected_competitors = StringField("selected_competitors", [Optional()])


class MSDRbdElastic(BaseMSDFilterParams):
    selected_competitors = StringField("selected_competitors", [Optional()])
    date_range_start = StringField(
        'date_range_start', date_range_rule("date_range_start"))
    date_range_end = StringField(
        'date_range_end', date_range_rule("date_range_end"))


class FareRevenueClassMix(BaseMSDFilterParams):
    selected_competitors = StringField("selected_competitors", [Optional()])
    selected_yearmonth = StringField(
        'selected_yearmonth', [Optional()] + date_range_rule("selected_yearmonth"))


class FareRevenueTrends(BaseMSDFilterParams):
    date_range_start = StringField(
        'date_range_start', date_range_rule("date_range_start"))
    date_range_end = StringField(
        'date_range_end', date_range_rule("date_range_end"))
    graph_carriers = StringField(
        "selected_competitors", comma_separated_characters_only("graph_carriers"))


class MSDFareRevenueDowRevenue(BaseMSDFilterParams):
    graph_carriers = StringField("selected_competitors",
                                 [*comma_separated_characters_only("graph_carriers"), Optional()])