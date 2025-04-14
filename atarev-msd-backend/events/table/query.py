from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from airports.entities import City, Country
from base.mongo_utils import convert_list_param_to_criteria, merge_criterions
from events.common.query import EventQuery
from events.table.form import EventTableForm


@dataclass
class EventTableQuery:
    host_code: str
    form: EventTableForm

    @property
    def query(self) -> Dict[str, Any]:
        match = EventQuery(
            host_code=self.host_code,
            location=self.form.get_origin() + self.form.get_destination(),
            is_country_market=self.form.is_country_market(),
            category=self.form.get_category(),
            sub_category=self.form.get_sub_category(),
        ).query

        return {**match, "event_year": self.form.year.data, "start_month": self.form.month.data}


@dataclass
class MinFareQuery:
    form: EventTableForm
    date_range: Tuple[int, int]
    host_code: str
    comp_code: Optional[str] = None

    @property
    def query(self):
        cabins = self.form.get_cabin(normalize=False)
        cabin = cabins[0] if cabins else None

        match = merge_criterions(
            [
                convert_list_param_to_criteria(
                    "carrierCode", [self.host_code, self.comp_code] if self.comp_code else self.host_code
                ),
                # fares for certain clients are stored using cities instead of airports
                # if markets is empty use city code instead,otherwise the query will take forever
                convert_list_param_to_criteria("marketOrigin", self.__origin_airports() or self.form.get_origin()),
                convert_list_param_to_criteria("marketDestination", self.__destination_airports() or self.form.get_destination()),
                {"$and": [{"outboundDate": {"$gte": self.date_range[0]}}, {"outboundDate": {"$lte": self.date_range[1]}}]},
            ]
        )

        pipeline = [
            {"$match": match},
            {"$unwind": {"path": "$lowestFares"}},
            {"$match": ({"lowestFares.cabinName": {"$regex": cabin, "$options": "i"}} if cabin else {})},
            {
                "$project": {
                    "market": {"$concat": ["$marketOrigin", "-", "$marketDestination"]},
                    "fare": "$lowestFares.fareAmount",
                    "currency": "$lowestFares.fareCurrency",
                    "cabin": "$lowestFares.cabinName",
                    "carrier_code": "$carrierCode",
                    "itineraries": 1,
                    "origin": "$marketOrigin",
                    "destination": "$marketDestination",
                }
            },
            {"$unwind": {"path": "$itineraries"}},
            {"$addFields": {"leg": {"$first": "$itineraries.legs"}}},
            {
                "$project": {
                    "_id": 0,
                    "market": 1,
                    "fare": 1,
                    "currency": 1,
                    "cabin": 1,
                    "carrier_code": 1,
                    "segment": {"$concat": ["$leg.legOriginCode", "-", "$leg.legDestCode"]},
                    "origin": 1,
                    "destination": 1,
                    "flt_num": "$leg.mkFltNum",
                    "outbound_date": "$leg.legDeptDate",
                    "dept_date": {
                        "$concat": [
                            {"$substr": ["$leg.legDeptDate", 0, 4]},
                            "-",
                            {"$substr": ["$leg.legDeptDate", 4, 2]},
                            "-",
                            {"$substr": ["$leg.legDeptDate", 6, 2]},
                        ]
                    },
                }
            },
        ]

        return pipeline

    def __origin_airports(self) -> List[str]:
        if Country(self.form.get_origin()[0]).is_valid():
            return Country(self.form.get_origin()[0]).airports()

        city_code = list(filter(lambda code: len(code) == 3, self.form.get_origin()))
        return list(City.get_city_airport_map(city_code).keys())

    def __destination_airports(self) -> List[str]:
        if Country(self.form.get_destination()[0]).is_valid():
            return Country(self.form.get_destination()[0]).airports()

        city_code = list(filter(lambda code: len(code) == 3, self.form.get_destination()))
        return list(City.get_city_airport_map(city_code).keys())
