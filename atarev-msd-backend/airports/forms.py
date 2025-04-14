from flask_wtf import FlaskForm
from wtforms import StringField
from wtforms.validators import DataRequired

class GetCountryCitiesForm(FlaskForm):
    country_code = StringField("country_code", [DataRequired()])