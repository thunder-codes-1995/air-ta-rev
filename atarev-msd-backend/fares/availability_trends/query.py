from dataclasses import dataclass
from datetime import datetime
from typing import List, Tuple, Union

from base.helpers.fields import Field
from base.helpers.user import User
from fares.availability_trends.forms import GetMinFareTrends
from fares.common.form import FareForm
from fares.common.query import Match
from fares.repository import FareRepository

fare_repo = FareRepository()


@dataclass
class GroupedMinFareQuery:
    user: User
    origin: List[str]
    destination: List[str]
    graph_carriers: List[str]
    flight_keys: List[str]
    date_range: Tuple[int, int]
    time_range: Tuple[int, int]
    connections: List[int]
    weekdays: List[int]
    duration: int
    flight_type: str
    cabin: str

    @property
    def query(self):

        match = Match(
            user=self.user,
            origin=self.origin,
            destination=self.destination,
            graph_carriers=self.graph_carriers,
            date_range=self.date_range,
            time_range=self.time_range,
            connections=self.connections,
            weekdays=self.weekdays,
            duration=self.duration,
            flight_type=self.flight_type,
        ).query

        exp = FareRepository.create_expiry_fares_date_match()
        pipeline = [
            {"$match": match},
            {"$unwind": {"path": "$lowestFares"}},
            {
                "$addFields": {
                    "belongsToCabin": {
                        "$regexMatch": {
                            "input": "$lowestFares.cabinName",
                            "regex": self.cabin,
                            "options": "i",
                        }
                    }
                }
            },
            {"$match": {"$or": FareRepository.create_expiry_fares_date_match()} if exp else {}},
            {"$match": {"belongsToCabin": True}},
            {"$addFields": {"outbound": {"$first": "$itineraries"}}},
            {"$addFields": {"inbound": {"$arrayElemAt": ["$itineraries", 1]}}},
            {"$addFields": {"outbound_leg": {"$first": "$outbound.legs"}}},
            {"$addFields": {"inbound_leg": {"$first": "$inbound.legs"}}},
            {"$addFields": {"is_connecting": {"$gt": [{"$size": "$outbound.legs"}, 1]}}},
            {
                "$match": (
                    {"$or": Match.handle_flight_keys("outbound_leg.opFltNum", self.flight_keys)} if self.flight_keys else {}
                )
            },
            {
                "$match": {
                    "$or": [
                        {"outbound_leg.legDeptDate": {"$gt": int(datetime.now().strftime("%Y%m%d"))}},
                        {
                            "$and": [
                                {"outbound_leg.legDeptDate": {"$eq": int(datetime.now().strftime("%Y%m%d"))}},
                                {"outbound_leg.legDeptTime": {"$gt": int(datetime.now().strftime("%H%M"))}},
                            ]
                        },
                    ],
                },
            },
            {"$sort": {"lowestFares.fareAmount": 1}},
            {
                "$group": {
                    "_id": {
                        "carrierCode": "$carrierCode",
                        "outboundDate": "$outboundDate",
                        **(
                            {
                                "fltNum": "$outbound_leg.mkFltNum",
                                "marketOrigin": "$marketOrigin",
                                "marketDestination": "$marketDestination",
                            }
                            if self.flight_keys
                            else {}
                        ),
                    },
                    "fares": {
                        "$push": {
                            "fareAmount": {"$toInt": {"$round": "$lowestFares.fareAmount"}},
                            "fareCurrency": "$lowestFares.fareCurrency",
                            "time": "$outbound.itinDeptTime",
                            "flightKey": "$flightKey",
                            "marketOrigin": "$marketOrigin",
                            "marketDestination": "$marketDestination",
                            "cabinName": "$lowestFares.cabinName",
                            "classCode": "$lowestFares.classCode",
                            "scrapeTime": "$lowestFares.scrapeTime",
                            "duration": "$outbound.itinDuration",
                            "fltNum": "$outbound_leg.mkFltNum",
                            "inFltNum": {"$cond": {"if": {"$eq": ["type", "RT"]}, "then": "$inbound_leg.mkFltNum", "else": None}},
                            "type": "$type",
                            "is_connecting": "$is_connecting",
                            "market": {"$concat": ["$marketOrigin", "-", "$marketDestination"]},
                            "connecting_flight_keys": {
                                "$cond": {
                                    "if": "$is_connecting",
                                    "then": {
                                        "$reduce": {
                                            "input": {"$slice": ["$outbound.legs", 1, {"$size": "$outbound.legs"}]},
                                            "initialValue": "",
                                            "in": {"$concat": ["$$value", {"$toString": "$$this.opFltNum"}, "-"]},
                                        }
                                    },
                                    "else": None,
                                }
                            },
                        }
                    },
                }
            },
            {
                "$project": {
                    "carrierCode": "$_id.carrierCode",
                    "outboundDate": "$_id.outboundDate",
                    "data": {"$first": "$fares"},
                    "_id": 0,
                }
            },
        ]

        return pipeline


@dataclass
class AvTrendsMatchQuery:
    form: Union[GetMinFareTrends, FareForm]
    user: User

    @property
    def query(self):

        pipelines = [
            *GroupedMinFareQuery(
                user=self.user,
                origin=self.form.get_origin(user=self.user),
                destination=self.form.get_destination(user=self.user),
                graph_carriers=self.form.get_graph_carriers(),
                date_range=self.form.get_date_range(),
                time_range=self.form.get_time_range(),
                connections=self.form.get_connection(),
                weekdays=self.form.get_weekdays(),
                duration=self.form.get_duration(),
                flight_type=self.form.get_ftype(self.user.carrier),
                flight_keys=self.form.get_flight_keys(),
                cabin=self.form.get_cabin(normalize=False)[0],
            ).query,
            *[
                {
                    "$project": {
                        "carrierCode": 1,
                        "outboundDate": 1,
                        "marketOrigin": "$data.marketOrigin",
                        "marketDestination": "$data.marketDestination",
                        "fltNum": 1,
                        "fareAmount": "$data.fareAmount",
                        "fareCurrency": "$data.fareCurrency",
                        "time": "$data.time",
                        "cabinName": "$data.cabinName",
                        "classCode": "$data.classCode",
                        "scrapeTime": "$data.scrapeTime",
                        "duration": "$data.duration",
                        "flightKey": "$data.flightKey",
                        "fltNum": "$data.fltNum",
                        "inFltNum": "$data.inFltNum",
                        "type": "$data.type",
                        "is_connecting": "$data.is_connecting",
                        "connecting_flight_keys": "$data.connecting_flight_keys",
                        "market": "$data.market",
                        **Field.date_as_string("departure_date", "outboundDate"),
                    }
                },
                {"$addFields": Field.weekday_from_str_date("weekday", "departure_date")},
            ],
        ]

        return pipelines
