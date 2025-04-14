import json
from typing import Dict, List, Literal, Tuple, TypedDict, Union

from wtforms import BooleanField, IntegerField, StringField
from wtforms.validators import Optional

from base.entities.exchange_rate import ExchangeRate
from base.entities.market import Bridge
from base.forms import Form
from configurations.repository import ConfigurationRepository
from utils.rules import comma_separated_characters_only

config_repo = ConfigurationRepository()

CHUNK_SIZE = 20


class ColumnOrder(TypedDict):
    sortKey: str
    sortOrder: Literal["asc", "desc"]


class FareForm(Form):
    orig_city_airport = StringField("orig_city_airport", comma_separated_characters_only("orig_city_airport"))
    dest_city_airport = StringField("dest_city_airport", comma_separated_characters_only("dest_city_airport"))
    graph_carriers = StringField("graph_carriers", [Optional(), *comma_separated_characters_only("graph_carriers")])
    weekdays = StringField("weekdays", [Optional()])
    time_range_start = StringField("time_range_start", [Optional()])
    time_range_end = StringField("time_range_end", [Optional()])
    cabin = StringField("cabin", [Optional()])
    flight = StringField("flight", [Optional(), *comma_separated_characters_only("flight")])
    currency = StringField("curency")
    connection = StringField("connection", [Optional()])
    duration = IntegerField("duration", [Optional()])
    overview = BooleanField("overview", [Optional()])
    page_ = IntegerField("page_", [Optional()])
    sort_ = StringField("sort_", [Optional()])
    type = StringField("type", [Optional()])

    def get_cabin(self, normalize: bool = False) -> List[str]:
        if not self.cabin.data:
            return ["ECO"]
        return super().get_cabin(normalize)

    def get_flight_keys(self) -> List[str]:
        if not self.flight.data:
            return []
        return self.flight.data.split(",")

    def get_connection(self) -> List[int]:
        if not self.connection.data or not self.connection.data.strip():
            return []
        return list(map(int, self.connection.data.strip().split(",")))

    def get_duration(self) -> int:
        if not self.duration.data:
            return 0
        return int(self.duration.data) * 100

    def get_weekdays(self) -> List[int]:
        if not self.weekdays.data:
            return []
        return list(map(int, self.weekdays.data.split(",")))

    def get_time_range(self) -> Tuple[int, int]:
        if self.time_range_start.data and self.time_range_end.data:
            return int(self.time_range_start.data.replace(":", "")), int(self.time_range_end.data.replace(":", ""))

        if self.time_range_start.data:
            return int(self.time_range_start.data.replace(":", "")), 2400

        if self.time_range_end.data:
            return 0, int(self.time_range_end.data.replace(":", ""))

        return (0, 2400)

    def should_convert_currency(self) -> bool:
        return bool(self.currency.data)

    def currency_conversion_rates(self, base_currencies: List[str]) -> Dict[str, float]:
        if not self.should_convert_currency():
            return {}

        rates = ExchangeRate(base_currencies, [self.currency.data]).rates()
        return rates

    def get_currency(self) -> Union[None, str]:
        if not self.currency.data:
            return None
        return self.currency.data.strip()

    def is_overview(self) -> bool:
        return self.overview.data

    def get_page(self) -> int:
        return self.page_.data or 1

    def offset(self) -> int:
        return (self.get_page() * CHUNK_SIZE) - CHUNK_SIZE

    def get_ftype(self, carrier_code: str) -> str:
        if not self.type.data:
            cm = Bridge(carrier_code, self.get_origin(), self.get_destination()).get_city_based_makret()
            markets = config_repo.get_by_key("MARKETS", carrier_code)
            markets = list(filter(lambda o: o["orig"] == cm.origin and o["dest"] == cm.destination, markets))

            if not markets:
                return "OW"
            return markets[0]["direction"]

        return self.type.data

    def get_ctype(self, carrier_code: str) -> str:
        if not self.currency.data:
            cm = Bridge(carrier_code, self.get_origin(), self.get_destination()).get_city_based_makret()
            markets = config_repo.get_by_key("MARKETS", carrier_code)
            markets = list(filter(lambda o: o["orig"] == cm.origin and o["dest"] == cm.destination, markets))
            return markets[0]["currency"]
        return self.currency.data

    def columns_order(self) -> List[ColumnOrder]:
        if not self.sort_.data:
            return []
        return json.loads(self.sort_.data)
