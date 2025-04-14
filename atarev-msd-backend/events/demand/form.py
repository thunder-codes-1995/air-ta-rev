from typing import Literal

from wtforms import StringField
from wtforms.validators import DataRequired

from base.forms import Form
from events.repository import EventRepository
from utils.rules import one_of

event_repo = EventRepository()


class EventDemandForm(Form):

    field = StringField("field", one_of("field", ["lf", "pax", "favg", "frev"]))
    orig_city_airport = StringField("orig_city_airport", [DataRequired()])
    dest_city_airport = StringField("dest_city_airport", [DataRequired()])

    def get_field(self) -> Literal["lf", "pax", "favg", "frev"]:
        return self.field.data
