from wtforms import StringField
from wtforms.validators import DataRequired

from dds.forms import BaseMSDFilterParams
from utils.rules import comma_separated_characters_only


class ConnectivityMap(BaseMSDFilterParams):
    selected_year = StringField('selected_year', validators=[DataRequired()])
    picked_airline = StringField('picked_airline', validators=[DataRequired()])
    bound_selection = StringField('bound_selection', validators=[DataRequired()])


class NetworkByondPoints(BaseMSDFilterParams):
    selected_year = StringField('selected_year', validators=[DataRequired()])


class NetworkSchedulingComparisonDetails(BaseMSDFilterParams):
    pass


class NetworkConictivityMap(BaseMSDFilterParams):
    selected_year = StringField('selected_year', validators=[DataRequired()])
    picked_airline = StringField('picked_airline', validators=[DataRequired()])
    bound_selection = StringField(
        'bound_selection', comma_separated_characters_only('bound_selection'))

