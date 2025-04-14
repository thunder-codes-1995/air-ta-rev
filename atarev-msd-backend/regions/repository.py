from typing import List

from base.repository import BaseRepository


class RegionRepository(BaseRepository):
    collection = "places"

    def get_region_by_cities(self, city_codes: List[str]):
        aggregation = [
            {"$match": {"countries.cities.city_code": {"$in": city_codes}}},
            {"$unwind": {"path": "$countries"}},
            {"$unwind": {"path": "$countries.cities"}},
            {"$unwind": {"path": "$countries.cities.airports"}},
            {
                "$project": {
                    "_id": 0,
                    "region_code": "$region_code",
                    "country_code": "$countries.country_code",
                    "country_name": "$countries.country_name",
                    "city_code": "$countries.cities.city_code",
                    "city_name": "$countries.cities.city_name",
                    "airport_code": "$countries.cities.airports.airport_code",
                    "airport_name": "$countries.cities.airports.airport_name",
                }
            },
        ]

        return self.aggregate(aggregation)

    def get_countries_grouped_by_region(self, region_codes: List[str] = None, country_codes: List[str] = None):
        aggregation = [{"$match": {"region_code": {"$in": region_codes}}}] if region_codes else []

        aggregation.append({"$unwind": {"path": "$countries"}})
        if country_codes:
            aggregation.append({"$match": {"countries.country_code": {"$in": country_codes}}})

        aggregation.append(
            {
                "$project": {
                    "_id": 0,
                    "region_code": "$region_code",
                    "country_code": "$countries.country_code",
                    "country_name": "$countries.country_name",
                }
            }
        )

        return self.aggregate(aggregation)
