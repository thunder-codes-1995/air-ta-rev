from wtforms import StringField
from wtforms.validators import DataRequired

from base.forms import Form


class RecommendationForm(Form):
    orig_city_airport = StringField("orig_city_airport", [DataRequired()])
    dest_city_airport = StringField("dest_city_airport", [DataRequired()])
    cabin = StringField("cabin", [DataRequired()])
