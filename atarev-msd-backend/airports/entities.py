from dataclasses import dataclass
from typing import Dict, List, Optional, cast

from airports.repository import AirportRepository

airport_repo = AirportRepository()


@dataclass
class Country:

    code: Optional[str] = None

    def airports(self, code: Optional[List[str]] = None) -> List[str]:
        assert bool(self.code) or bool(code), "country code is mendetory"
        c = airport_repo.get_country_airports(code or [cast(str, self.code)])
        return list(c)

    def is_valid(self) -> bool:
        assert self.code, "Country code is required"
        if len(self.code) != 2:
            return False
        return bool(airport_repo.find_one({"country_code": self.code}))

    @classmethod
    def airport_country_map(cls, airport_code: List[str]) -> Dict[str, str]:
        cursor = airport_repo.find({"airport_iata_code": {"$in": airport_code}})
        return {item["airport_iata_code"]: item["country_code"] for item in cursor}


@dataclass
class City:

    code: Optional[str] = None

    def airports(self, code: Optional[List[str]] = None) -> List[str]:
        assert bool(self.code) or bool(code), "city code is mendetory"
        c = airport_repo.find({"city_code": self.code or code})
        return [rec["airport_iata_code"] for rec in c]

    @classmethod
    def get_city_country_map(self, cities: List[str]) -> Dict[str, str]:
        agg = [
            {"$match": {"city_code": {"$in": cities}}},
            {"$group": {"_id": {"country_code": "$country_code", "city_code": "$city_code"}}},
            {"$project": {"_id": 0, "country_code": "$_id.country_code", "city_code": "$_id.city_code"}},
        ]
        c = airport_repo.aggregate(agg)
        return {obj["city_code"]: obj["country_code"] for obj in c}

    @classmethod
    def get_city_airport_map(self, cities: List[str]) -> Dict[str, str]:
        agg = [
            {"$match": {"city_code": {"$in": cities}}},
            {"$group": {"_id": {"airport_code": "$airport_iata_code", "city_code": "$city_code"}}},
            {"$project": {"_id": 0, "airport_code": "$_id.airport_code", "city_code": "$_id.city_code"}},
        ]

        c = airport_repo.aggregate(agg)
        return {obj["airport_code"]: obj["city_code"] for obj in c}
