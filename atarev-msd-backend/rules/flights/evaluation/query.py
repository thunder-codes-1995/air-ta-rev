from dataclasses import dataclass
from datetime import datetime
from typing import List

from airports.entities import City
from base.helpers.cabin import CabinMapper
from base.mongo_utils import convert_list_param_to_criteria, merge_criterions
from fares.repository import FareRepository
from rules.flights.evaluation.form import RuleEvaluationForm


@dataclass
class FaresQuery:
    form: RuleEvaluationForm

    @property
    def query(self):
        match = merge_criterions(
            [
                convert_list_param_to_criteria("hostCode", self.form.host_code),
                convert_list_param_to_criteria("outboundDate", {"$gte": int(datetime.now().strftime("%Y%m%d"))}),
                convert_list_param_to_criteria("marketOrigin", list(set([self.form.origin, *City(self.form.origin).airports()]))),
                convert_list_param_to_criteria(
                    "marketDestination", list(set([self.form.destination, *City(self.form.destination).airports()]))
                ),
            ]
        )

        exp = FareRepository.create_expiry_fares_date_match()

        pipeline = [
            {"$match": match},
            {"$match": {"$or": FareRepository.create_expiry_fares_date_match()} if exp else {}},
            {"$unwind": {"path": "$lowestFares"}},
            {"$match": {"lowestFares.cabinName": {"$regex": CabinMapper.humanize(self.form.cabin), "$options": "i"}}},
            {
                "$match": {
                    "$and": [
                        {"lowestFares.lastUpdateDateTime": {"$gte": self.form.start_datetime}},
                        {"lowestFares.lastUpdateDateTime": {"$lte": self.form.end_datetime}},
                    ]
                }
            },
            {"$addFields": {"itin": {"$first": "$itineraries"}}},
            {"$addFields": {"leg": {"$first": "$itin.legs"}}},
            {
                "$project": {
                    "_id": 0,
                    "carrier_code": "$carrierCode",
                    "origin": "$marketOrigin",
                    "destination": "$marketDestination",
                    "cabin": "$lowestFares.cabinName",
                    "class": "$lowestFares.classCode",
                    "departure_date": "$itin.itinDeptDate",
                    "departure_time": "$itin.itinDeptTime",
                    "arrival_date": "$itin.itinArrivalDate",
                    "arrival_time": "$itin.itinArrivalTime",
                    "flt_num": "$leg.opFltNum",
                    "fare": "$lowestFares.fareAmount",
                    "currency": "$lowestFares.fareCurrency",
                    "is_connecting": {"$cond": {"if": {"$gte": [{"$size": "$itineraries"}, 2]}, "then": True, "else": False}},
                    "op_code": {
                        "$reduce": {
                            "input": {"$map": {"input": "$itin.legs", "in": "$$this.opAlCode"}},
                            "initialValue": "",
                            "in": {
                                "$cond": {
                                    "if": {"$eq": ["$$value", ""]},
                                    "then": "$$this",
                                    "else": {"$concat": ["$$value", "-", "$$this"]},
                                }
                            },
                        }
                    },
                    "mk_code": {
                        "$reduce": {
                            "input": {"$map": {"input": "$itin.legs", "in": "$$this.mkAlCode"}},
                            "initialValue": "",
                            "in": {
                                "$cond": {
                                    "if": {"$eq": ["$$value", ""]},
                                    "then": "$$this",
                                    "else": {"$concat": ["$$value", "-", "$$this"]},
                                }
                            },
                        }
                    },
                }
            },
        ]

        return pipeline


@dataclass
class SchedQuery:
    form: RuleEvaluationForm
    start_date: int
    end_date: int

    @property
    def query(self):
        return merge_criterions(
            [
                convert_list_param_to_criteria("opAlCode", self.form.host_code),
                convert_list_param_to_criteria("originCode", list(set([self.form.origin, *City(self.form.origin).airports()]))),
                convert_list_param_to_criteria(
                    "destCode", list(set([self.form.destination, *City(self.form.destination).airports()]))
                ),
                {
                    "$and": [
                        {"deptDate": {"$gte": self.start_date}},
                        {"deptDate": {"$lte": self.end_date}},
                    ]
                },
            ]
        )


@dataclass
class RemoveOldRecommendationsQuery:
    identifiers: List[str]

    @property
    def query(self):
        return merge_criterions(
            [
                convert_list_param_to_criteria("identifier", {"$in": self.identifiers}),
                {"type": {"$ne": "E"}},
            ]
        )
