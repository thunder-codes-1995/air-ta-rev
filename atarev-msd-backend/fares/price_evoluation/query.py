from dataclasses import dataclass
from typing import List, Tuple

from base.helpers.user import User
from fares.common.query import Match
from fares.price_evoluation.forms import GetPriceEvolution


@dataclass
class HistoricalGroupedMinFareQuery:
    user: User
    origin: List[str]
    destination: List[str]
    graph_carriers: List[str]
    flight_keys: List[str]
    time_range: Tuple[int, int]
    connections: List[int]
    weekdays: List[int]
    duration: int
    flight_type: str
    cabin: str
    date: int

    @property
    def query(self):

        match = Match(
            user=self.user,
            origin=self.origin,
            destination=self.destination,
            graph_carriers=self.graph_carriers,
            time_range=self.time_range,
            connections=self.connections,
            weekdays=self.weekdays,
            duration=self.duration,
            flight_type=self.flight_type,
            date=self.date,
        ).query

        pipelines = [
            {"$match": match},
            {"$unwind": {"path": "$historicalFares"}},
            {
                "$addFields": {
                    "belongsToCabin": {
                        "$regexMatch": {
                            "input": "$historicalFares.cabinName",
                            "regex": self.cabin,
                            "options": "i",
                        }
                    }
                }
            },
            {"$match": {"belongsToCabin": True}},
            {"$addFields": {"time": {"$first": "$itineraries"}}},
            {"$addFields": {"time": "$time.itinDeptTime"}},
            {"$addFields": {"outbound": {"$first": "$itineraries"}}},
            {"$addFields": {"inbound": {"$arrayElemAt": ["$itineraries", 1]}}},
            {"$addFields": {"leg": {"$first": "$outbound.legs"}}},
            {"$addFields": {"inbound_leg": {"$first": "$inbound.legs"}}},
            {"$addFields": {"fltNum": "$leg.mkFltNum"}},
            {"$addFields": {"is_connecting": {"$gt": [{"$size": "$outbound.legs"}, 1]}}},
            {"$match": ({"$or": Match.handle_flight_keys("leg.opFltNum", self.flight_keys)} if self.flight_keys else {})},
            {
                "$project": {
                    "flightKey": 1,
                    "carrierCode": 1,
                    "marketOrigin": 1,
                    "marketDestination": 1,
                    "outboundDate": 1,
                    "fareAmount": {"$toInt": {"$round": "$historicalFares.fareAmount"}},
                    "fareCurrency": "$historicalFares.fareCurrency",
                    "scrapeTime": "$historicalFares.lastUpdateDateTime",
                    "dtd": "$historicalFares.dtd",
                    "cabinName": "$historicalFares.cabinName",
                    "classCode": "$historicalFares.classCode",
                    "is_connecting": "$is_connecting",
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
                    "duration": "$outbound.itinDuration",
                    "inFltNum": {"$cond": {"if": {"$eq": ["type", "RT"]}, "then": "$inbound_leg.mkFltNum", "else": None}},
                    "market": {"$concat": ["$marketOrigin", "-", "$marketDestination"]},
                    "type": "$type",
                    "time": 1,
                    "fltNum": 1,
                    "_id": 0,
                }
            },
            {"$sort": {"insertDate": -1, "fareAmount": 1}},
            {
                "$group": {
                    "_id": {
                        "dtd": "$dtd",
                        "carrierCode": "$carrierCode",
                        **(
                            {
                                "flightKey": "$flightKey",
                                "marketOrigin": "$marketOrigin",
                                "marketDestination": "$marketDestination",
                            }
                            if self.flight_keys
                            else {}
                        ),
                    },
                    "fares": {
                        "$push": {
                            "flightKey": "$flightKey",
                            "carrierCode": "$carrierCode",
                            "marketOrigin": "$marketOrigin",
                            "marketDestination": "$marketDestination",
                            "outboundDate": "$outboundDate",
                            "fareAmount": "$fareAmount",
                            "fareCurrency": "$fareCurrency",
                            "dtd": "$dtd",
                            "duration": "$duration",
                            "is_connecting": "$is_connecting",
                            "connecting_flight_keys": "$connecting_flight_keys",
                            "cabinName": "$cabinName",
                            "time": "$time",
                            "fltNum": "$fltNum",
                            "scrapeTime": "$scrapeTime",
                            "inFltNum": "$inFltNum",
                            "market": "$market",
                            "type": "$type",
                            "classCode": "$classCode",
                        }
                    },
                }
            },
            {"$addFields": {"data": {"$first": "$fares"}}},
        ]

        return pipelines


@dataclass
class PriceEvoluationQuery:
    form: GetPriceEvolution
    user: User

    @property
    def query(self):

        pipelines = [
            *HistoricalGroupedMinFareQuery(
                self.user,
                origin=self.form.get_origin(user=self.user),
                destination=self.form.get_destination(user=self.user),
                graph_carriers=self.form.get_graph_carriers(),
                date=self.form.get_norm_date(),
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
                        "_id": 0,
                        "flightKey": "$data.flightKey",
                        "carrierCode": "$data.carrierCode",
                        "marketOrigin": "$data.marketOrigin",
                        "marketDestination": "$data.marketDestination",
                        "outboundDate": "$data.outboundDate",
                        "fareAmount": "$data.fareAmount",
                        "fareCurrency": "$data.fareCurrency",
                        "dtd": "$data.dtd",
                        "cabinName": "$data.cabinName",
                        "time": "$data.time",
                        "fltNum": "$data.fltNum",
                        "scrapeTime": "$data.scrapeTime",
                        "duration": "$data.duration",
                        "is_connecting": "$data.is_connecting",
                        "connecting_flight_keys": "$data.connecting_flight_keys",
                        "inFltNum": "$data.inFltNum",
                        "market": "$data.market",
                        "type": "$data.type",
                        "classCode": "$data.classCode",
                    }
                },
                {"$sort": {"outboundDate": 1}},
                {"$match": {"dtd": {"$gte": int(self.form.get_dtd())}}},
            ],
        ]

        return pipelines
