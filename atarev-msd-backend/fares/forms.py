from wtforms import StringField
from wtforms.validators import DataRequired, Optional

from base.forms import Form
from utils.rules import comma_separated_characters_only


class GetFareStructureTable(Form):
    graph_carriers = StringField("graph_carriers", [*comma_separated_characters_only("graph_carriers"), Optional()])
    orig_city_airport = StringField("orig_city_airport", [DataRequired()])
    dest_city_airport = StringField("dest_city_airport", [DataRequired()])
