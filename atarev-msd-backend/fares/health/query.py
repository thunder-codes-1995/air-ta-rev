from dataclasses import dataclass
from datetime import date, timedelta

from flask import request

from base.mongo_utils import convert_list_param_to_criteria, merge_criterions
from fares.health.forms import TrackFares
from fares.repository import FareRepository

repo = FareRepository()


@dataclass
class FareCountQuery:
    form: TrackFares
    only_valid: bool

    @property
    def query(self):
        today = date.today()
        days_forward = today + timedelta(days=self.form.get_days())
        match = merge_criterions(
            [
                convert_list_param_to_criteria("marketOrigin", self.form.get_origin()),
                convert_list_param_to_criteria("marketDestination", self.form.get_destination()),
                convert_list_param_to_criteria("carrierCode", self.form.get_carriers()),
                convert_list_param_to_criteria("hostCode", self.form.get_hosts()),
                {
                    "$and": [
                        {"outboundDate": {"$gte": int(today.strftime("%Y%m%d"))}},
                        {"outboundDate": {"$lte": int(days_forward.strftime("%Y%m%d"))}},
                    ]
                },
            ]
        )

        return [
            {"$match": match},
            {"$match": {"$or": repo.create_expiry_fares_date_match()} if self.only_valid else {}},
            {"$unwind": {"path": "$lowestFares"}},
            {
                "$project": {
                    "updated_at": "$lowestFares.scrapeTime",
                    "origin": "$marketOrigin",
                    "destination": "$marketDestination",
                    "cabin": "$lowestFares.cabinName",
                    "carrierCode": "$carrierCode",
                    "outboundDate": "$outboundDate",
                }
            },
            {"$sort": {"updated_at": -1}},
            {
                "$group": {
                    "_id": {
                        "origin": "$origin",
                        "destination": "$destination",
                        "carrierCode": "$carrierCode",
                        "outboundDate": "$outboundDate",
                        "cabin": "$cabin",
                    },
                    "fares": {
                        "$push": {
                            "origin": "$origin",
                            "destination": "$destination",
                            "carrierCode": "$carrierCode",
                            "outboundDate": "$outboundDate",
                            "updated_at": "$updated_at",
                            "cabin": "$cabin",
                        }
                    },
                }
            },
            {"$project": {"_id": 0, "fare": {"$first": "$fares"}}},
            {
                "$project": {
                    "origin": "$fare.origin",
                    "destination": "$fare.destination",
                    "outboundDate": "$fare.outboundDate",
                    "cabin": "$fare.cabin",
                    "carrierCode": "$fare.carrierCode",
                    "updated_at": "$fare.updated_at",
                }
            },
            {
                "$match": {
                    "lowestFares.cabinName": {
                        "$regex": f".*{request.args.get('cabin')}.*$",
                        "$options": "i",
                    }
                }
                if self.form.get_cabin()
                else {}
            },
            {
                "$group": {
                    "_id": {
                        "origin": "$origin",
                        "destination": "$destination",
                        "carrierCode": "$carrierCode",
                    },
                    "count": {"$sum": 1},
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "marketOrigin": "$_id.origin",
                    "marketDestination": "$_id.destination",
                    "carrierCode": "$_id.carrierCode",
                    "count": 1,
                }
            },
        ]
