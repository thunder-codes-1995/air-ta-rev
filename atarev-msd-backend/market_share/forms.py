from dds.forms import BaseMSDFilterParams
from utils.rules import comma_separated_characters_only, date_range_rule, one_of


from wtforms import StringField
from wtforms.validators import DataRequired


class MsdMarketShareTrends(BaseMSDFilterParams):
    time_direction = StringField('time_direction', validators=[DataRequired()])
    date_range_start = StringField(
        'date_range_start', date_range_rule("date_range_start"))
    date_range_end = StringField(
        'date_range_end', date_range_rule("date_range_end"))


class MarketShareTrends(MsdMarketShareTrends):
    graph_carriers = StringField(
        "selected_competitors", comma_separated_characters_only("graph_carriers"))


class MarketShareFareAvg(BaseMSDFilterParams):
    time_direction = StringField('time_direction',
                                 one_of('time_direction', ["historical", "forward", "historical-forward"]))
    graph_carriers = StringField(
        "selected_competitors", comma_separated_characters_only("graph_carriers"))