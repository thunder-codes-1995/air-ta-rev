from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional

from base.entities.market import AirportBasedMakret, Bridge, CityBasedMarket
from base.helpers.theme import Handler, Theme
from configurations.repository import ConfigurationRepository
from filters.repository import FilterRepository, Market

config_repo = ConfigurationRepository()
filter_repo = FilterRepository()


@dataclass
class Carrier:
    host_code: str

    def airport(self, origin: str, destination: str) -> AirportBasedMakret:
        return AirportBasedMakret(self.host_code, origin, destination)

    def city(self, origin: str, destination: str) -> CityBasedMarket:
        return CityBasedMarket(self.host_code, origin, destination)

    def bridge(self, origins: List[str], destinations: List[str]) -> Bridge:
        return Bridge(self.host_code, origins, destinations)

    def carrier_colors(self, origins: List[str], destinations: List[str], theme: Theme) -> Dict[str, str]:
        competitors = self.bridge(origins, destinations).get_city_based_makret().competitors()
        colors = Handler(theme).palette(self.host_code)

        if len(competitors) > len(colors):
            colors += ["#ffffff"] * (len(competitors) - len(colors))

        return {code: colors[idx] for idx, code in enumerate(competitors)}

    def competitors(self, main_only: bool = False) -> Dict[str, List[str]]:
        markets = config_repo.get_by_key("COMPETITORS", self.host_code)
        if main_only:
            {f"{market['origin']}-{market['destination'] }": market["competitors"] for market in markets}
        return {f"{market['origin']}-{market['destination'] }": market["competitors"][0] for market in markets}

    def city_based_markets(
        self,
        origin_in: Optional[Iterable[str]] = None,
        destination_in: Optional[Iterable[str]] = None,
    ) -> Iterable[Market]:
        """return all supported city based marker for a specific client code"""
        c = filter_repo.get_customer_markets(
            customer=self.host_code,
            origin_in=origin_in,
            destination_in=destination_in,
        )
        return c

    def airports_based_markets(self) -> List[Market]:
        c = config_repo.get_by_key("MARKETS", self.host_code)
        return [{"orig": market["orig"], "dest": market["dest"]} for market in c]
