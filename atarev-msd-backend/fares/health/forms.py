from typing import List

from wtforms import IntegerField, StringField
from wtforms.validators import Optional

from base.forms import Form
from utils.rules import comma_separated_characters_only


class TrackFares(Form):
    cabin = StringField("cabin", [Optional()])
    orig_city_airport = StringField("orig_city_airport", [*comma_separated_characters_only("orig_city_airport"), Optional()])
    dest_city_airport = StringField("dest_city_airport", [*comma_separated_characters_only("orig_city_airport"), Optional()])
    carriers = StringField("carrier", [*comma_separated_characters_only("carrier"), Optional()])
    hosts = StringField("hosts", [*comma_separated_characters_only("carrier"), Optional()])
    days = IntegerField("days", [Optional()])

    def get_days(self) -> int:
        return self.days.data or 30

    def get_carriers(self) -> List[str]:
        if not self.carriers.data or "all" in self.carriers.data.lower():
            return []
        return self.carriers.data.split(",")

    def get_hosts(self) -> List[str]:
        if not self.hosts.data or "all" in self.hosts.data.lower():
            return []
        return self.hosts.data.split(",")

    def get_markets(self):
        origins, destinations = self.get_origin(), self.get_destination()
        if len(origins) == len(destinations):
            return list(zip(origins, destinations))
        return [(origin, destination) for origin in origins for destination in destinations]
