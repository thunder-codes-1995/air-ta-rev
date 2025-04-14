from dataclasses import dataclass
from typing import Optional

from airports.repository import AirportRepository
from base.entities.carrier import Carrier
from events.repository import EventRepository
from users.repository import UserRepository

event_repo = EventRepository()
airport_rep = AirportRepository()
user_repository = UserRepository()


@dataclass
class CountryCity:
    host_code: str
    origin: Optional[str] = None
    lookup: str = None


    def dict_markets(self, markets):
        market_dict = {}
        for route in markets:
            market_dict.setdefault(route['orig'], []).append(route['dest'])
        return market_dict

    def get_destination_by_origin(self, origin):
        # if origin exists in market_dict get origin
        # else get all cities from country
        # basically it checks if the origin is a country or a city
        cities = airport_rep.get_country_cities(origin) if not self.market_dict.get(
            origin) else [origin]
        des = set()
        for city in cities:
            markets = self.market_dict.get(city, [])
            if markets:
                # get intersection
                des = set(markets) & self.destination_set
        return des

    def get(self, origin=None):
        c = list(Carrier(self.host_code).city_based_markets())

        self.market_dict = self.dict_markets(c)

        origin_set = set([item["orig"] for item in c])

        self.destination_set = set([item["dest"] for item in c])

        if origin:
            destination_set = set()
            for orig in origin.split(","):
                destination_set |= self.get_destination_by_origin(orig)
        else:
            destination_set = self.destination_set

        origin_grouped = airport_rep.aggregate(self.pipeline(list(origin_set)))
        destination_grouped = airport_rep.aggregate(
            self.pipeline(list(destination_set)))

        return {
            "origins": list(origin_grouped),
            "destinations": list(destination_grouped)
        }

    def pipeline(self, cities):
        if not self.lookup:
            self.lookup = ""
        else:
            self.lookup = self.lookup.upper()
        return [
            {
                '$match': {
                    'city_code': {
                        '$in': cities
                    }
                }
            },
            {
                '$match': {
                    '$or': [
                        {
                            'country_code': {
                                '$regex': f'^{self.lookup}',
                                '$options': 'i'
                            }
                        }, {
                            'city_code': {
                                '$regex': f'^{self.lookup}',
                                '$options': 'i'
                            }
                        }
                    ]
                }
            },
            {
                '$group': {
                    '_id': '$country_code',
                    'country_code': {
                        '$first': '$country_code'
                    },
                    'country_name': {
                        '$first': '$country_name'
                    },
                    'cities': {
                        '$addToSet': {
                            'city_code': '$city_code',
                            'city_name': '$city_name'
                        }
                    }
                }
            }, {
                '$sort': {
                    'country_code': 1
                }
            }, {
                '$project': {
                    '_id': 0,
                    'country_code': 1,
                    'country_name': 1,
                    'cities': {
                        '$function': {
                            'body': 'function(cities) { return cities.sort((a, b) => (a.city_code > b.city_code) ? 1 : -1); }',
                            'args': [
                                '$cities'
                            ],
                            'lang': 'js'
                        }
                    }
                }
            }
        ]
