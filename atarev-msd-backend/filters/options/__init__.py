from collections import defaultdict
from dataclasses import dataclass
from typing import Iterable, List, Optional, Dict, Any

from airports.entities import City
from airports.repository import AirportRepository
from base.entities.carrier import Carrier
from base.helpers.user import User
from events.repository import EventRepository
from filters.forms import FilterOptionsForm
from filters.options.query import CategoryOptionsQuery
from filters.options.types import Category, Option, SimpleOnD
from users.repository import UserRepository

event_repo = EventRepository()
airport_rep = AirportRepository()
user_repository = UserRepository()


def cabin_options() -> List[Option]:
    return [{"value": "ECONOMY"}, {"value": "BUSINESS"}]


def labelize(value):
    return value.replace("_", " ").title().strip()


@dataclass
class CategoryOption:
    user: User
    form: FilterOptionsForm
    event_ids: list = None
    group_ids: list = None

    def __get_location(self):
        return self.form.get_origin("origin_city") + self.form.get_destination("destination")

    def __get_location_match(self):

        country_code = list(filter(lambda code: len(code) == 2, self.__get_location()))
        city_code = list(filter(lambda code: len(code) == 3, self.__get_location()))
        _or: List[Dict[str, Any]] = []
        if country_code:
            _or.append({"country_code": {"$in": country_code}})
        if city_code:
            countries = list(City.get_city_country_map(city_code).values())
            _or.append({
                    "$or": [
                        {
                            "$and": [
                                {"city": {"$regex": f"^({'|'.join(city_code)})", "$options": "i"}},
                                {
                                    "categories": {
                                        "$not": {
                                            "$elemMatch": {
                                                "category": {"$in": ["national", "school"]},
                                            },
                                        },
                                    },
                                },
                            ]
                        },
                        {"city": None, "country_code": {"$in": countries}},
                    ]
        })
        return {"$or": _or}

    def get(self) -> List[Category]:
        res = []
        user = user_repository.get_user(self.user.username)

        event_ids = user["selected_filter_options"].get("selected_event_ids", [])
        location_match = self.__get_location_match()
        pipeline = CategoryOptionsQuery(
            host_code=self.user.carrier, event_ids=event_ids, location_match=location_match, lookup=self.form.lookup.data
        ).query
        c = list(event_repo.aggregate(pipeline))

        for category in sorted(c, key=lambda x: x["category"]):
            sub_categories = category["sub_categories"]
            sorted_subs = sorted([sub for sub in sub_categories if sub], key=lambda x: x["value"])

            sub = list(filter(bool, sorted_subs))
            sub_categories = []

            for sub_category in sub:
                for idx, event in enumerate(sub_category["events"]):
                    if isinstance(event, list):
                        sub_category["events"].extend(event)
                        del sub_category["events"][idx]
                sub_categories.append({"label": labelize(sub_category["value"]), **sub_category})

            res.append({"label": labelize(category["category"]), "value": category["category"], "sub_categories": sub_categories})
        return res

    def post(self):

        if self.group_ids:
            events = event_repo.find({"group_id": {"$in": self.group_ids}, "airline_code": self.user.carrier})
            if self.event_ids:
                self.event_ids.extend([str(event["_id"]) for event in events])
            else:
                self.event_ids = [str(event["_id"]) for event in events]
        if self.event_ids:
            user_repository.store_filter_options(username=self.user.username, **{"selected_event_ids": self.event_ids})
            return f"{len(self.event_ids)} events selected"
        else:
            user_repository.store_filter_options(username=self.user.username, **{"selected_event_ids": []})

        return "0 events selected"


@dataclass
class CityOptions:
    host_code: str
    hierarchical: bool  # use it later
    origin_in: Optional[Iterable[str]] = None
    destination_in: Optional[Iterable[str]] = None

    def get(self) -> SimpleOnD:
        return self.simple()

    def simple(self) -> SimpleOnD:
        c = Carrier(self.host_code).city_based_markets(self.origin_in, self.destination_in)

        origin = []
        destination = []

        for item in c:
            origin.append(item["orig"])
            destination.append(item["dest"])

        return {"origin": [{"value": val} for val in set(origin)], "destination": [{"value": val} for val in set(destination)]}
