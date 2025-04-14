from dataclasses import dataclass
from datetime import date, timedelta

from flask import request

from base.mongo_utils import convert_list_param_to_criteria, merge_criterions

from ..forms import MsdGetCosBreakdown, PosBreakDownTables


@dataclass
class COSBreakDownTableQuery:
    form: PosBreakDownTables

    @property
    def query(self):
        today = date.today()
        eight_weeks_in_past = today - timedelta(days=8 * 7)
        match = merge_criterions(
            [
                convert_list_param_to_criteria("orig_code", self.form.get_origin()),
                convert_list_param_to_criteria("dest_code", self.form.get_destination()),
            ]
        )

        return [
            {
                "$match": {
                    "$and": [
                        {"travel_date": {"$gte": int(eight_weeks_in_past.strftime("%Y%m%d"))}},
                        {"travel_date": {"$lte": int(today.strftime("%Y%m%d"))}},
                    ],
                    **match,
                }
            },
            {
                "$group": {
                    "_id": {
                        "country_of_sale": "$country_of_sale",
                        "dom_op_al_code": "$dom_op_al_code",
                        "origin": "$orig_code",
                        "destination": "$dest_code",
                        "origin_city": "$city_orig_code",
                        "destination_city": "$city_dest_code",
                        "travel_date": "$travel_date",
                    },
                    "pax": {"$sum": "$pax"},
                    "blended_fare": {"$sum": "$blended_fare"},
                }
            },
            {
                "$project": {
                    "country_of_sale": "$_id.country_of_sale",
                    "origin": "$_id.origin",
                    "destination": "$_id.destination",
                    "origin_city": "$_id.origin_city",
                    "destination_city": "$_id.destination_city",
                    "dom_op_al_code": "$_id.dom_op_al_code",
                    "travel_date": "$_id.travel_date",
                    "blended_fare": 1,
                    "_id": 0,
                    "pax": 1,
                }
            },
        ]


@dataclass
class COSBreakDownFiguresQuery:
    form: MsdGetCosBreakdown

    @property
    def query(self):
        match = merge_criterions(
            [
                convert_list_param_to_criteria("orig_code", self.form.get_origin()),
                convert_list_param_to_criteria("dest_code", self.form.get_destination()),
                convert_list_param_to_criteria("dom_op_al_code", [*self.form.get_selected_competitors(), request.user.carrier]),
                convert_list_param_to_criteria("travel_year", self.form.year()),
            ]
        )

        return [
            {"$match": match},
            {
                "$group": {
                    "_id": {
                        "country_of_sale": "$country_of_sale",
                        "dom_op_al_code": "$dom_op_al_code",
                    },
                    "pax": {"$sum": "$pax"},
                    "blended_fare": {"$sum": "$blended_fare"},
                }
            },
            {
                "$project": {
                    "country_of_sale": "$_id.country_of_sale",
                    "dom_op_al_code": "$_id.dom_op_al_code",
                    "blended_fare": 1,
                    "_id": 0,
                    "pax": 1,
                }
            },
        ]
