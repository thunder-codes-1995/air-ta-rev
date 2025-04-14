from flask_wtf import FlaskForm
from wtforms import StringField

from utils.rules import one_of


class EventFieldForm(FlaskForm):
    target = StringField("target", one_of("target", ["table", "calendar"]))
