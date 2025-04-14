import importlib

from wtforms import StringField, FieldList
from wtforms.validators import DataRequired, Optional

from flask_wtf import FlaskForm

from base.forms import Form
from utils.rules import one_of, comma_separated_characters_only



class FilterOptionsForm(Form):
    target = StringField("target", [Optional(), *one_of("target", ["ea", "all"])])
    origin_city = StringField("origin_city", [Optional()])
    destination = StringField("destination", [Optional()])
    lookup = StringField("lookup", [Optional()])
    country_city_lookup = StringField("lookup", [Optional()])



class MarketFilterOptions(FlaskForm):
    lookup = StringField("lookup", [DataRequired()])
    orig_city_airport = StringField("orig_city_airport", [Optional()])
    dest_city_airport = StringField("dest_city_airport", [Optional()])


class GetCustomerMarketsForm(FlaskForm):
    customer = StringField("customer", [DataRequired()])
