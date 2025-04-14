from datetime import date

from flask import request

from base.mongo_utils import convert_list_param_to_criteria, merge_criterions
from fares.availability_trends.forms import GetMinFareTrends
from fares.forms import GetFareStructureTable
from utils.funcs import split_string


class FareBuilder:
    def _get_market_host_conditions(self, form: GetMinFareTrends):
        conditions = [
            convert_list_param_to_criteria("marketOrigin", form.get_orig_city_airports_list()),
            convert_list_param_to_criteria("marketDestination", form.get_dest_city_airports_list()),
            convert_list_param_to_criteria("carrierCode", form.get_graph_carriers()),
            {"hostCode": request.user.carrier},
        ]
        return conditions

    def get_market_host_match(self, form: GetMinFareTrends):
        conditions = self._get_market_host_conditions(form)
        return merge_criterions(conditions)

    def get_min_fare_match(self, form: GetMinFareTrends):
        user_markets = request.user.markets
        conditions = self._get_market_host_conditions(form)

        _and = []
        _or = []

        if user_markets:
            _or.extend([{"$and": [{"marketOrigin": market[0]}, {"marketDestination": market[1]}]} for market in user_markets])

        _and = [
            *_and,
            *[
                {"outboundDate": {"$gte": form.get_start_date(as_int=True)}},
                {"outboundDate": {"$lte": form.get_end_date(as_int=True)}},
            ],
        ]

        if _or:
            conditions.append({"$or": _or})

        if request.args.get("duration"):
            start_duration = int(request.args.get("duration")) * 100
            conditions.append({"itineraries.0.itinDuration": {"$gte": start_duration}})

        if form.get_connection():
            conditions.append(
                {"$or": [{"itineraries.0.legs": {"$size": connection + 1}} for connection in form.get_connection()]}
            )

        if request.args.get("flight"):
            _or = self.__build_flights_match()
            conditions.append({"$or": _or})

        if request.args.get("time_range_start") and request.args.get("time_range_end"):
            _and = [
                *_and,
                *[
                    {"itineraries.0.itinDeptTime": {"$gte": int(request.args.get("time_range_start").replace(":", ""))}},
                    {"itineraries.0.itinDeptTime": {"$lte": int(request.args.get("time_range_end").replace(":", ""))}},
                ],
            ]

        if request.args.get("weekdays"):
            weeks = [int(item) for item in request.args.get("weekdays").split(",")]
            _and.append({"outboundDayOfWeek": {"$in": weeks}})

        if _and:
            conditions.append({"$and": _and})

        return merge_criterions(conditions)

    def __build_flights_match(self):
        _or = []
        for string in split_string(request.args.get("flight"), allow_empty=False):
            _or.append(
                {
                    "$and": [
                        {"itineraries.0.legs.0.mkFltNum": int(string[2:])},
                        {"carrierCode": string[0:2]},
                    ]
                }
            )
        return _or

    def fare_structure_table_pipeline(self, form: GetFareStructureTable):
        conditions = merge_criterions(
            [
                convert_list_param_to_criteria("orig_code", form.get_origin()),
                convert_list_param_to_criteria("dest_code", form.get_destination()),
            ]
        )

        return [
            {"$match": conditions},
            {
                "$project": {
                    "_id": 0,
                    "pax_ratio": 1,
                    "fare_basis": 1,
                    "base_fare": 1,
                    "dom_op_al_code": 1,
                    "orig_code": 1,
                    "dest_code": 1,
                    "currency": 1,
                    "yqyr": {"$cond": [{"$eq": ["$yqyr", None]}, 0, "$yqyr"]},
                    "non_refundable": 1,
                    "rbkd": {"$substr": ["$fare_basis", 0, 1]},
                    "max_stay": {"$cond": [{"$eq": ["$max_stay", None]}, "-", "$max_stay"]},
                    "min_stay": {"$cond": [{"$eq": ["$min_stay", None]}, "-", "$min_stay"]},
                    "ap": {"$cond": [{"$eq": ["$ap", None]}, "-", "$ap"]},
                    "ow_rt": {"$cond": [{"$eq": ["$ow_rt", None]}, "-", "$ow_rt"]},
                }
            },
            {
                "$addFields": {
                    "total_fare_conv": {"$add": ["$base_fare", "$yqyr"]},
                }
            },
            {"$match": {"ow_rt": {"$ne": 1}}},
            {"$addFields": {"ow_rt": {"$cond": [{"#eq": ["$ow_rt", 2]}, "RT", "OW"]}}},
        ]

    def get_pax_rbkd_match(self, form: GetFareStructureTable):
        return merge_criterions(
            [
                convert_list_param_to_criteria("orig_code", form.get_origin()),
                convert_list_param_to_criteria("dest_code", form.get_destination()),
                # convert_list_param_to_criteria('dom_op_al_code',form.get_selected_competitors_list()),
                convert_list_param_to_criteria("dom_op_al_code", form.get_graph_carriers()),
                {"travel_year": date.today().year},
            ]
        )
