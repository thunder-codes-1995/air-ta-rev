from typing import Dict, List, Optional, TypedDict, Union

from base.helpers.duration import Duration
from base.repository import BaseRepository


class UniqueCityCode(TypedDict):
    value: str


class GroupedByOrderObj(TypedDict):
    city_name: str
    city_code: str
    country_code: str
    airport_name: str
    airport_iata_code: str
    type: str
    country_name: str


class AirportRepository(BaseRepository):
    collection = "airports"

    def get_airports_coordinates_map(self, airport_codes: List[str]):
        cursor = self.aggregate(
            [
                {"$match": {"airport_iata_code": {"$in": airport_codes}, "latitude": {"$ne": None}, "longitude": {"$ne": None}}},
                {"$project": {"_id": 0, "airport_code": "$airport_iata_code", "latitude": 1, "longitude": 1}},
            ]
        )
        return {airport["airport_code"]: (airport["latitude"], airport["longitude"]) for airport in cursor}

    def get_coordinates(self, airport_code: str):
        """get lat,long based on airport-code EX : WDS"""
        redis_key: str = f"airports_coordinates_{airport_code}"

        if self.redis.get(redis_key):
            coordinates = self.redis.get(redis_key)
            return coordinates["latitude"], coordinates["longitude"]

        key: str = self.get_column("airport_iata_code")
        result = list(
            self.aggregate(
                [
                    {"$match": {key: airport_code, "latitude": {"$ne": None}, "longitude": {"$ne": None}}},
                    {"$project": {"_id": 0, "latitude": 1, "longitude": 1}},
                ]
            )
        )

        if len(result) > 0:
            pt = result[0]
            self.redis.set(redis_key, {"latitude": float(pt.get("latitude")), "longitude": float(pt.get("longitude"))})
            return float(pt.get("latitude")), float(pt.get("longitude"))
        return None

    def get_country(self, airport_code: str) -> str:
        """get country code based on airport-code EX : WDS"""
        key: str = self.get_column("airport_iata_code")
        redis_key: str = f"airport_country_{airport_code}"

        if self.redis.get(redis_key):
            return self.redis.get(redis_key)

        airport = self.stringify(self.find_one({key: airport_code}))
        self.redis.set(redis_key, airport.get("country_code"), Duration.days(1))
        return airport.get("country_code")

    def get_country_airport_map(self, codes: Optional[List[str]] = None) -> Dict[str, GroupedByOrderObj]:
        """
        get a list of airport codes and return a dict
        representing each airport as key and data for that airport as value
        ex : {"AMS" : {...}}
        """
        codes = codes or []
        codes.sort()

        pipeline = [
            {
                "$project": {
                    "_id": {"$toString": "_id"},
                    "airport_iata_code": 1,
                    "airport_name": 1,
                    "airport_country": 1,
                    "city_code": 1,
                    "city_name": 1,
                    "country_code": 1,
                    "country_name": 1,
                    "type": 1,
                }
            }
        ]
        if codes:
            pipeline.insert(0, {"$match": {"airport_iata_code": {"$in": codes}}})

        cursor = self.aggregate(pipeline)
        mp = {record["airport_iata_code"]: record for record in cursor}
        return mp

    def get_airport_coord_map(self, codes: List[str] = []):
        """
        get a list of airport codes and return a dict
        representing each airport as key and data for that airport as value
        ex : {"BBB" : {...}}
        """
        codes = codes or []
        codes.sort()
        cache_key = "city_coord_map" + "_".join(codes)
        if self.redis.get(cache_key):
            return self.redis.get(cache_key)

        pipeline = [
            {"$lookup": {"from": "cities", "localField": "country_code", "foreignField": "country_code", "as": "cities"}},
            {"$unwind": {"path": "$cities"}},
            {
                "$group": {
                    "_id": {
                        "city_code": "$city_code",
                        "airport_code": "$airport_iata_code",
                        "latitude": "$cities.latitude",
                        "longitude": "$cities.longitude",
                    }
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "city_code": "$_id.city_code",
                    "latitude": "$_id.latitude",
                    "longitude": "$_id.longitude",
                    "airport_code": "$_id.airport_code",
                }
            },
        ]

        if codes:
            pipeline.insert(0, {"$match": {"airport_iata_code": {"$in": codes}}})
        cursor = self.aggregate(pipeline)
        mp = {record["airport_code"]: record for record in cursor}

        self.redis.set(cache_key, mp, expiration_in_seconds=Duration.days(1))
        return mp

    def normalized_country_map(self, codes: List[str]):
        """takes list of country list and return dict of their codes"""
        codes = codes or []
        codes.sort()
        cache_key = "normalized_country_map" + "_".join(codes)
        if self.redis.get(cache_key):
            return self.redis.get(cache_key)

        pipeline = [
            {"$match": {"country_name": {"$in": codes}}},
            {
                "$project": {
                    "country_name": 1,
                    "country_code": 1,
                }
            },
        ]

        cursor = self.aggregate(pipeline)
        _map = {record["country_name"]: record["country_code"] for record in cursor}

        self.redis.set(cache_key, _map, expiration_in_seconds=Duration.days(1))
        return _map

    def get_airports_for_cities(self, cities_list: List[str]):
        """get airports based on city code"""
        return self.aggregate(
            [
                {
                    "$match": {
                        "city_code": {"$in": cities_list},
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                    }
                },
            ]
        )

    def get_city_code_for_airport(self, airport_code: Union[str, List[str]]):
        if type(airport_code) == str:
            redis_key: str = f"airports_city_{airport_code}"
            if self.redis.get(redis_key):
                return self.redis.get(redis_key)

            result = self.find_one({"airport_iata_code": airport_code, "type": {"$ne": "METROPOLITAN"}})
            if not result:
                return
        else:
            redis_key: str = f"airports_city_{'_'.join(airport_code)}"
            if self.redis.get(redis_key):
                return self.redis.get(redis_key)

            result = self.find_one({"airport_iata_code": {"$in": airport_code}, "type": {"$ne": "METROPOLITAN"}})

        self.redis.set(redis_key, self.stringify(result), expiration_in_seconds=Duration.months(1))
        return result

    def get_countries_for_airports(self, airports: List[str] = []):
        cursor = self.find({"airport_iata_code": {"$in": airports}} if airports else {})
        return {item["airport_iata_code"]: item["country_code"] for item in cursor}

    def get_countries_for_cities(self, cities: List[str]):
        if cities:
            cursor = self.find({"city_code": {"$in": cities}})
            return set([item["country_code"] for item in cursor])
        else:
            return []

    def filter_cities_by_country(self, cities: List[str], country_code: str):
        cursor = self.find({"city_code": {"$in": cities}, "country_code": country_code} if cities else {})
        return set([item["city_code"] for item in cursor])

    def get_airports_grouped_by_city(self, match={}, as_map=True):
        aggregation = [
            {
                "$match": {"type": {"$ne": "METROPOLITAN"}, "disabled": {"$ne": True}, **match},
            },
            {
                "$group": {
                    "_id": {
                        "city_code": "$city_code",
                        "city_name": "$city_name",
                    },
                    "airports": {"$push": {"airport_name": "$airport_name", "airport_code": "$airport_iata_code"}},
                }
            },
            {"$project": {"_id": 0, "city_code": "$_id.city_code", "city_name": "$_id.city_name", "airports": 1}},
        ]

        if not as_map:
            return self.aggregate(aggregation)
        res = {}
        for obj in self.aggregate(aggregation):
            res[obj["city_code"]] = obj["airports"]
        return res

    def get_all_countries(self) -> Dict[str, List]:
        # cache_key = 'all_countries'
        # cached = self.redis.get(cache_key)
        # if cached:
        #     return cached

        pipeline = [
            {"$group": {"_id": "$country_code", "country_name": {"$first": "$country_name"}}},
            {
                "$project": {
                    "_id": 0,
                    "country_code": "$_id",
                    "country_name": 1,
                }
            },
        ]
        countries: List[Dict] = list(self.aggregate(pipeline))
        result = {"countries": countries}
        return result

    def get_country_cities(self, country_code: Optional[str] = None) -> Dict[str, List]:
        pipeline = [
            {"$match": {"country_code": country_code} if country_code else {}},
            {"$group": {"_id": "$city_code"}},
            {"$project": {"_id": 0, "city_code": "$_id"}},
        ]

        # if no country code, send 30 cities
        if not country_code:
            pipeline.append({"$limit": 30})

        cities = list(self.aggregate(pipeline))
        cities = [city["city_code"] for city in cities]

        return cities

    def get_country_airports(self, country_code: Optional[List[str]] = None) -> List[str]:
        pipeline = [
            {"$match": {"country_code": {"$in": country_code}} if country_code else {}},
            {"$group": {"_id": "$airport_iata_code"}},
            {"$project": {"_id": 0, "airport_iata_code": "$_id"}},
        ]

        # if no country code, send 30 airports
        if not country_code:
            pipeline.append({"$limit": 30})

        airports = list(self.aggregate(pipeline))
        airports = [airport["airport_iata_code"] for airport in airports]

        return airports

    def get_country_code_by_city_code(self, city_code: str) -> Union[str, None]:
        doc = self._db[self.collection].find_one({"city_code": city_code})
        if not doc:
            return None
        return doc.get("country_code")

    def get_country_code_by_airport_code(self, airport_code: str) -> Union[str, None]:
        doc = self._db[self.collection].find_one({"airport_iata_code": airport_code})
        if not doc:
            return None
        return doc.get("country_code")

    def get_city_code_by_airport_code(self, airport_code: str) -> Union[str, None]:
        doc = self._db[self.collection].find_one({"airport_iata_code": airport_code})
        if not doc:
            return None
        return doc.get("city_code")

    def is_valid_city_code(self, city_code: str) -> bool:
        doc = self._db[self.collection].find_one({"city_code": city_code})
        return bool(doc)

    def is_valid_country_code(self, country_code: str) -> bool:
        doc = self._db[self.collection].find_one({"country_code": country_code})
        return bool(doc)

    def check_airport_code_valid(self, airport_code: str) -> bool:
        doc = self._db[self.collection].find_one({"airport_iata_code": airport_code})
        return bool(doc)

    def get_airports_grouped_by_country(self, codes: List[str] = []):
        codes = codes or []

        pipeline = [
            {"$group": {"_id": {"country_code": "$country_code"}, "airports": {"$push": "$airport_iata_code"}}},
            {"$project": {"country_code": "$_id.country_code", "_id": 0, "airports": 1}},
        ]
        if codes:
            pipeline.insert(0, {"$match": {"airport_iata_code": {"$in": codes}}})

        return self.aggregate(pipeline)
