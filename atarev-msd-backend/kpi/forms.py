from dds.forms import BaseMSDFilterParams


from wtforms import StringField
from wtforms.validators import DataRequired


class MsdKpiForm(BaseMSDFilterParams):
    kpi_type = StringField('kpi_type', validators=[DataRequired()])
    pass