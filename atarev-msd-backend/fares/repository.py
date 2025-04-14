from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional

from flask import request

from base.helpers.fields import Field
from base.repository import BaseRepository
from configurations.repository import ConfigurationRepository


class FareRepository(BaseRepository):
    collection = "fares2"
    configuration_repository = ConfigurationRepository()

    @classmethod
    def create_expiry_fares_date_match(self):
        """get only up-to-date fares based on some criterias in our db"""
        today = datetime.today()
        yesterday = today - timedelta(days=1)
        fare_expiry_rules = self.configuration_repository.get_by_key("SCRAPED_FARE_EXPIRY_RULES", request.user.carrier)

        return [
            {
                "$and": [
                    {
                        "$and": [
                            {
                                "outboundDate": {
                                    "$gte": int(
                                        (yesterday + timedelta(days=rule["daysToDepartureMin"])).date().strftime("%Y%m%d")
                                    )
                                }
                            },
                            {
                                "outboundDate": {
                                    "$lte": int(
                                        (yesterday + timedelta(days=rule["daysToDepartureMax"])).date().strftime("%Y%m%d")
                                    )
                                }
                            },
                        ]
                    },
                    {
                        "lowestFares.lastUpdateDateTime": {
                            "$gte": int((today - timedelta(hours=rule["expireInHours"])).strftime("%Y%m%d%H%M%S"))
                        }
                    },
                ]
            }
            for rule in fare_expiry_rules
            if rule["expireInHours"] > 0
        ]

    def get(
        self,
        match: Optional[Dict[str, Any]] = None,
        cabin: Optional[str] = None,
        execlud_expired: bool = True,
        include: Optional[List[str]] = None,
        limit: Optional[int] = None,
        flatten: bool = False,
    ):

        m = {
            **(match or {}),
            **({"$or": self.create_expiry_fares_date_match()} if execlud_expired else {}),
        }

        if cabin or flatten:
            pipeline: List[Dict[str, Any]] = [
                {"$match": m},
                {"$unwind": {"path": "$lowestFares"}},
                {"$match": ({"lowestFares.cabinName": {"$regex": cabin, "$options": "i"}} if cabin else {})},
                {"$addFields": {"itin": {"$first": "$itineraries"}}},
                {"$addFields": {"leg": {"$first": "$itin.legs"}}},
                {
                    "$project": (
                        {
                            "_id": 0,
                            "cabin": "$lowestFares.cabinName",
                            **Field.date_as_string("str_departure_date", "outboundDate"),
                            **{f: 1 for f in include},
                        }
                        if include
                        else {
                            "_id": 0,
                            "carrier_code": "$carrierCode",
                            "origin": "$marketOrigin",
                            "destination": "$marketDestination",
                            "cabin": "$lowestFares.cabinName",
                            "departure_date": "$outboundDate",
                            "flt_num": "$leg.opFltNum",
                            "fare": "$lowestFares.fareAmount",
                            "currency": "$lowestFares.fareCurrency",
                            **Field.date_as_string("str_departure_date", "outboundDate"),
                        }
                    )
                },
                {"$sort": {"lowestFares.scrapeTime": -1}},
            ]

        else:
            pipeline = [
                {"$match": m},
                {"$addFields": {"itin": {"$first": "$itineraries"}}},
                {"$addFields": {"leg": {"$first": "$itin.legs"}}},
                {
                    "$project": (
                        {"_id": 0, **Field.date_as_string("str_departure_date", "outboundDate"), **{f: 1 for f in include}}
                        if include
                        else {
                            "_id": 0,
                            "carrier_code": "$carrierCode",
                            "origin": "$marketOrigin",
                            "destination": "$marketDestination",
                            "fares": "$lowestFares",
                            "departure_date": "$outboundDate",
                            "flt_num": "$leg.opFltNum",
                            **Field.date_as_string("str_departure_date", "outboundDate"),
                        }
                    )
                },
            ]

        if limit:
            assert limit > 0, f"invalud limit value {limit}"
            pipeline.append({"$limit": int(limit)})

        return self.aggregate(pipeline)

    def get_flights(self, match, only_valid: bool):
        expiry = self.create_expiry_fares_date_match()
        pipeline = [
            {"$match": match},
            {"$match": {"$or": expiry} if (only_valid and expiry) else {}},
            {"$addFields": {"outbound": {"$first": "$itineraries"}}},
            {"$addFields": {"leg": {"$first": "$outbound.legs"}}},
            {
                "$group": {
                    "_id": {
                        "carrierCode": "$carrierCode",
                        "fltNum": "$leg.mkFltNum",
                    }
                }
            },
            {
                "$project": {
                    "carrierCode": "$_id.carrierCode",
                    "fltNum": "$_id.fltNum",
                    "_id": 0,
                }
            },
        ]

        return self.aggregate(pipeline)

    def get_fares_breakdown(self, match):
        pipelines = [
            {"$match": match if match else {}},
            {"$unwind": {"path": "$lowestFares"}},
            {
                "$project": {
                    "cabin": "$lowestFares.cabinName",
                    "class": "$lowestFares.classCode",
                    "base": "$lowestFares.baseFare",
                    "tax": "$lowestFares.taxAmount",
                    "yqyr": "$lowestFares.yqyrAmount",
                    "origin": "$marketOrigin",
                    "destination": "$marketDestination",
                    "dept_date": "$outboundDate",
                    "flight_key": "$flightKey",
                    "_id": 0,
                }
            },
        ]

        return self.aggregate(pipelines)


class FareStructureRepository(BaseRepository):
    collection = "fs"

    def get_fare_structure(
        self,
        carrier_code: str,
        origin: Iterable[str],
        destination: Iterable[str],
        cabins: Optional[Iterable[str]] = None,
        fare_basis: Optional[Iterable[str]] = None,
    ):
        pipelines = [
            {"$match": {"airline_code": carrier_code, "origin": {"$in": origin}, "destination": {"$in": destination}}},
            {"$unwind": {"path": "$cabins"}},
            {"$match": {"cabins.code": {"$in": cabins}} if cabins else {}},
            {"$unwind": {"path": "$cabins.classes"}},
            {
                "$project": {
                    "_id": 0,
                    "classes": "$cabins.classes",
                    "cabin": "$cabins.code",
                    "origin": "$origin",
                    "destination": "$destination",
                    "date": "$date",
                }
            },
            {"$unwind": {"path": "$classes.fares"}},
            {"$match": {"classes.fares.fare_basis": {"$in": fare_basis}} if fare_basis else {}},
            {
                "$project": {
                    "class": "$classes.code",
                    "base": "$classes.fares.base_fare",
                    "surcharge": "$classes.fares.surcharge_amt",
                    "q": "$classes.fares.q",
                    "yq": "$classes.fares.yq",
                    "yr": "$classes.fares.yr",
                    "fare_basis": "$classes.fares.fare_basis",
                    "origin": 1,
                    "destination": 1,
                    "date": 1,
                    "cabin": 1,
                    "currency": "$classes.fares.currency",
                    "q_currency": "$classes.fares.q_currency",
                    "yq_currency": "$classes.fares.yq_currency",
                    "yr_currency": "$classes.fares.yr_currency",
                    "_id": 0,
                }
            },
        ]

        return self.aggregate(pipelines)


class FSRepository(BaseRepository):
    collection = "fares_structure"
