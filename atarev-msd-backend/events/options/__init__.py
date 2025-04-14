from dataclasses import dataclass
from typing import List, TypedDict

from flask import Request

from airports.repository import AirportRepository
from filters.options import CategoryOption, CityOptions
from filters.options.types import Category, Option

airport_repository = AirportRepository()


class OptionResp(TypedDict):
    categories: List[Category]
    cities: List[Option]


@dataclass
class Option:
    request: Request
    host_code: str

    def get(self) -> OptionResp:
        categories = CategoryOption(host_code=self.host_code).get()
        ond = CityOptions(host_code=self.host_code, hierarchical=False).get()
        origin = [item["value"] for item in ond["origin"]]
        destination = [item["value"] for item in ond["destination"]]
        _all = list(set(origin + destination))
        country_arg = self.request.args.get("country")
        countries = airport_repository.get_countries_for_cities(_all)
        if country_arg:
            _all = list(airport_repository.filter_cities_by_country(_all, country_arg))
        cities = [{"value": val} for val in _all]
        return {"categories": categories, "cities": cities, "countries": [{"value": val} for val in list(countries)]}
