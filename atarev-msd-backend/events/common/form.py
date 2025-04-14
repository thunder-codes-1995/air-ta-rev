from typing import Dict, List

from wtforms import StringField
from wtforms.validators import Optional

from airports.entities import City, Country
from airports.repository import AirportRepository, GroupedByOrderObj
from base.forms import Form
from utils.rules import comma_separated_characters_only

airport_repo = AirportRepository()


class EventForm(Form):
    orig_city_airport = StringField("orig_city_airport", comma_separated_characters_only("orig_city_airport"))
    dest_city_airport = StringField("dest_city_airport", comma_separated_characters_only("dest_city_airport"))
    cabin = StringField("cabin", [Optional()])
    category = StringField("category", validators=[Optional()])
    sub_category = StringField("sub_category", validators=[Optional()])

    def __grouped_by_airport(self, airports: List[str]) -> Dict[str, GroupedByOrderObj]:
        grouped_by_airport = airport_repo.get_country_airport_map(airports)
        return grouped_by_airport

    def get_airport_country_map(self) -> Dict[str, str]:
        origin = self.get_origin()[0]
        destination = self.get_destination()[0]

        if self.is_country_market():
            origin_airports = Country(origin).airports()
            destination_airports = Country(destination).airports()
        else:
            origin_airports = City(origin).airports()
            destination_airports = City(destination).airports()

        grouped_by_airport = self.__grouped_by_airport(list(set(origin_airports + destination_airports)))
        return {airport: grouped_by_airport[airport]["country_code"] for airport in grouped_by_airport}

    def get_category(self) -> List[str]:
        return self.list_of_string("category")

    def get_sub_category(self) -> List[str]:
        return self.list_of_string("sub_category")

    def should_consider_stats(self) -> bool:
        origin_city = list(filter(lambda code: len(code) == 3, self.get_origin()))
        dest_city = list(filter(lambda code: len(code) == 3, self.get_destination()))
        return bool(origin_city) and bool(dest_city) and len(origin_city) == 1 and len(dest_city) == 1
