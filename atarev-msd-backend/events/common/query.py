from dataclasses import dataclass
from typing import Any, Dict, List, Tuple, Union

from bson import ObjectId

from airports.entities import City, Country
from base.mongo_utils import convert_list_param_to_criteria, merge_criterions


@dataclass
class EventQuery:
    host_code: str
    location: List[str]
    is_country_market: bool
    category: Union[List[str], None] = None
    sub_category: Union[List[str], None] = None
    event_ids: Union[List[str], None] = None

    @property
    def query(self) -> Dict[str, Any]:
        match = merge_criterions([convert_list_param_to_criteria("airline_code", self.host_code)])

        if self.category:
            match["categories.category"] = {"$in": self.category}

        if self.sub_category:
            match["categories.sub_categories"] = {"$in": self.sub_category}

        if self.event_ids:
            match["_id"] = {"$in": [ObjectId(_id) for _id in self.event_ids]}

        return {**match, **self.__event_location_match()}

    def __event_location_match(self) -> Dict[str, Any]:
        country_code = list(filter(lambda code: len(code) == 2, self.location))
        city_code = list(filter(lambda code: len(code) == 3, self.location))
        _or: List[Dict[str, Any]] = []

        if country_code:
            _or.append({"country_code": {"$in": country_code}})

        if city_code:
            countries = list(City.get_city_country_map(city_code).values())
            _or.append(
                {
                    "$or": [
                        {"city": {"$regex": f"^({'|'.join(city_code)})", "$options": "i"}},
                        {"city": None, "country_code": {"$in": countries}},
                    ]
                }
            )

        return {"$or": _or}


@dataclass
class LoadFactorQuery:
    host_code: str
    origin: List[str]
    destination: List[str]
    is_country_market: bool
    date_range: Tuple[int, int]

    @property
    def query(self):

        return merge_criterions(
            [
                convert_list_param_to_criteria("airline_code", self.host_code),
                convert_list_param_to_criteria("origin", self.__origin_airports()),
                convert_list_param_to_criteria("destination", self.__destination_airports()),
                {
                    "$and": [
                        {"departure_date": {"$gte": self.date_range[0]}},
                        {"departure_date": {"$lte": self.date_range[1]}},
                    ]
                },
            ]
        )

    def __origin_airports(self) -> List[str]:
        if Country(self.origin[0]).is_valid():
            return Country(self.origin[0]).airports()

        city_code = list(filter(lambda code: len(code) == 3, self.origin))
        return list(City.get_city_airport_map(city_code).keys())

    def __destination_airports(self) -> List[str]:
        if Country(self.destination[0]).is_valid():
            return Country(self.destination[0]).airports()

        city_code = list(filter(lambda code: len(code) == 3, self.destination))
        return list(City.get_city_airport_map(city_code).keys())
