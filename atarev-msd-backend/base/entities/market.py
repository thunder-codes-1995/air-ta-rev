from dataclasses import dataclass, field
from typing import Dict, List, TypedDict, cast

from airports.repository import AirportRepository
from base.helpers.carrier import Handler as Carriers
from base.helpers.theme import Theme
from configurations.repository import ConfigurationRepository
from filters.repository import FilterRepository

airport_repo = AirportRepository()
filter_repo = FilterRepository()
config_repo = ConfigurationRepository()


class AirportMarket(TypedDict):
    origin: str
    destination: str


@dataclass
class City:
    code: str

    def airports(self) -> List[str]:
        c = airport_repo.find({"city_code": self.code})
        return [item["airport_iata_code"] for item in c]


@dataclass
class CityBasedMarket:
    carrier_code: str
    origin: str
    destination: str

    def competitors(self, include_host: bool = True):
        market = filter_repo.get_market(self.origin, self.destination, self.carrier_code)
        return market.get("competitors", []) if not include_host else [self.carrier_code, *market.get("competitors", [])]

    def carrier_color_map(self, theme: Theme) -> Dict[str, str]:
        comps = self.competitors()
        return Carriers(comps).colors(theme)

    def airport_based_markets(self) -> List[AirportMarket]:
        res = []
        origin_airports = City(self.origin).airports()
        destination_airports = City(self.destination).airports()

        for origin in origin_airports:
            for destinaton in destination_airports:
                market = cast(AirportMarket, {"origin": origin, "destination": destinaton})
                res.append(market)
        return res

        # i commented out the section below because some clients have their market configuration as city code
        # for mkt in all_markets:
        #     if mkt["orig"] in origin_airports and mkt["dest"] in destination_airports:
        #         market = cast(AirportMarket, {"origin": mkt["orig"], "destination": mkt["dest"]})
        #         res.append(market)

        # return res


@dataclass
class AirportBasedMakret:
    carrier_code: str
    origin: str
    destination: str

    def currency(self) -> str:
        return config_repo.get_market_currency(self.carrier_code, f"{self.origin})-{self.destination}", "USD")

    def competitors(self) -> List[str]:
        markets = config_repo.get_by_key("COMPETITORS", self.carrier_code)
        targeted = list(
            filter(lambda market: market["origin"] == self.origin and market["destination"] == self.destination, markets)
        )
        assert len(targeted) == 1, f"invalid airport based market {self.origin}-{self.destination}"
        return targeted[0]["competitors"]


@dataclass
class Bridge:
    carrier_code: str
    source: List[str] = field(default_factory=list)
    destination: List[str] = field(default_factory=list)

    def get_city_based_makret(self) -> CityBasedMarket:
        origin_city = airport_repo.get_city_code_for_airport(self.source)["city_code"]
        destination_city = airport_repo.get_city_code_for_airport(self.destination)["city_code"]
        return CityBasedMarket(self.carrier_code, origin_city, destination_city)
