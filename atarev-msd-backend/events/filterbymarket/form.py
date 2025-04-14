from datetime import datetime
from typing import List
from wtforms import StringField
from wtforms.validators import DataRequired

from events.common.form import EventForm
from events.repository import EventRepository

event_repo = EventRepository()


class FilterByMarketForm(EventForm):
    date = StringField("date", [DataRequired()])

    def __init__(self, *args, **kwargs):
        super(FilterByMarketForm, self).__init__(*args, **kwargs)
        delattr(self, 'cabin')

    def validate_date(cls, data):
        try:
            datetime.strptime(data.data, '%Y-%m-%d')
            return data
        except ValueError:
            raise ValueError(f"{data.data} is not date compatible (YYYY-MM-DD)")

    def get_origins(self) -> List[str]:
        return self.list_of_string("orig_city_airport")

    def get_destinations(self) -> List[str]:
        return self.list_of_string("dest_city_airport")

    def get_all_cities(self) -> List[str]:
        return list(set(self.get_origins() + self.get_destinations()))


