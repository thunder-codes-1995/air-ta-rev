from flask_wtf import FlaskForm
from wtforms import StringField
from wtforms.validators import DataRequired

from utils.rules import (
    one_of,
)


class GetConfigurationByKey(FlaskForm):
    key = StringField("key", one_of('key', ['MSD_LAST_HISTORICAL_UPDATED', 'MSD_LAST_FORWORD_UPDATED']))


class GetCustomerMarketsForm(FlaskForm):
    customer = StringField("customer", [DataRequired()])



