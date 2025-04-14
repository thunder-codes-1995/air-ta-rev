from flask import request

from base.mongo_utils import convert_list_param_to_criteria, merge_criterions
from network.forms import NetworkByondPoints, NetworkConictivityMap, NetworkSchedulingComparisonDetails


class NetworkBuilder:
    def beyond_points_common_pipeline(self, form: NetworkByondPoints):
        match = merge_criterions(
            [
                convert_list_param_to_criteria("orig_code", form.get_orig_regions_list()),
                convert_list_param_to_criteria("dest_code", form.get_dest_regions_list()),
                convert_list_param_to_criteria("orig_code", form.get_orig_country_list()),
                convert_list_param_to_criteria("dest_code", form.get_dest_country_list()),
                convert_list_param_to_criteria("orig_code", form.get_orig_city_airports_list()),
                convert_list_param_to_criteria("dest_code", form.get_dest_city_airports_list()),
                convert_list_param_to_criteria("seg_class", form.get_cabins_list()),
                convert_list_param_to_criteria("country_of_sale", form.get_point_of_sales_list()),
                convert_list_param_to_criteria("distribution_channel", form.get_sales_channels_list()),
                convert_list_param_to_criteria("is_group", form.get_pax_type()),
                convert_list_param_to_criteria("dom_op_al_code", [*form.get_selected_competitors_list(), request.user.carrier]),
                {"travel_year": int(form.selected_year.data)},
            ]
        )

        return [
            {"$match": match},
            {"$match": {"dom_op_al_code": {"$ne": "OTH"}}},
        ]

    def beyond_points_summery_pipeline(self, form: NetworkByondPoints):
        return [
            *self.beyond_points_common_pipeline(form),
            {
                "$group": {
                    "_id": {
                        "is_direct": "$is_direct",
                    },
                    "pax": {"$sum": "$pax"},
                    "blended_rev": {"$sum": "$blended_rev"},
                }
            },
            {
                "$project": {
                    "is_direct": {"$cond": {"if": {"$eq": ["$_id.is_direct", True]}, "then": "Direct", "else": "Indirect"}},
                    "pax": 1,
                    "blended_rev": {"$toInt": {"$round": ["$blended_rev", 0]}},
                    "blended_fare": {"$toInt": {"$round": [{"$divide": ["$blended_rev", "$pax"]}, 0]}},
                }
            },
        ]

    def beyond_points_inbound_summery_pipeline(self, form: NetworkByondPoints):
        return [
            *self.beyond_points_common_pipeline(form),
            {"$match": {"bound": {"$in": ["other", "inbound"]}, "prev_dest": {"$ne": "-"}}},
            {
                "$group": {
                    "_id": {
                        "dom_op_al_code": "$dom_op_al_code",
                        "prev_dest": "$prev_dest",
                        "orig_code": "$orig_code",
                    },
                    "pax": {"$sum": "$pax"},
                    "blended_rev": {"$sum": "$blended_rev"},
                    "blended_fare": {"$avg": "$blended_fare"},
                }
            },
            {
                "$project": {
                    "dom_op_al_code": "$_id.dom_op_al_code",
                    "prev_dest": "$_id.prev_dest",
                    "orig_code": "$_id.orig_code",
                    "pax": 1,
                    "blended_rev": {"$toInt": {"$round": ["$blended_rev", 0]}},
                    "blended_fare": {"$toInt": {"$round": ["$blended_fare", 0]}},
                }
            },
            {"$sort": {"pax": -1}},
            {"$limit": 50},
        ]

    def beyond_points_outbound_summery_pipeline(self, form: NetworkByondPoints):
        return [
            *self.beyond_points_common_pipeline(form),
            {"$match": {"bound": {"$in": ["other", "outbound"]}, "next_dest": {"$ne": "-"}}},
            {
                "$group": {
                    "_id": {
                        "dom_op_al_code": "$dom_op_al_code",
                        "next_dest": "$next_dest",
                        "dest_code": "$dest_code",
                    },
                    "pax": {"$sum": "$pax"},
                    "blended_rev": {"$sum": "$blended_rev"},
                    "blended_fare": {"$avg": "$blended_fare"},
                }
            },
            {
                "$project": {
                    "dom_op_al_code": "$_id.dom_op_al_code",
                    "next_dest": "$_id.next_dest",
                    "dest_code": "$_id.dest_code",
                    "pax": 1,
                    "blended_rev": {"$toInt": {"$round": ["$blended_rev", 0]}},
                    "blended_fare": {"$toInt": {"$round": ["$blended_fare", 0]}},
                }
            },
            {"$sort": {"pax": -1}},
            {"$limit": 50},
        ]

    def network_comparison_details_pipeline(self, form: NetworkSchedulingComparisonDetails):
        year, month = form.get_selected_year_month()
        match = merge_criterions(
            [
                convert_list_param_to_criteria("orig_code", form.get_orig_regions_list()),
                convert_list_param_to_criteria("dest_code", form.get_dest_regions_list()),
                convert_list_param_to_criteria("orig_code", form.get_orig_country_list()),
                convert_list_param_to_criteria("dest_code", form.get_dest_country_list()),
                convert_list_param_to_criteria("orig_code", form.get_orig_city_airports_list()),
                convert_list_param_to_criteria("dest_code", form.get_dest_city_airports_list()),
                convert_list_param_to_criteria("seg_class", form.get_cabins_list()),
                convert_list_param_to_criteria("country_of_sale", form.get_point_of_sales_list()),
                convert_list_param_to_criteria("distribution_channel", form.get_sales_channels_list()),
                convert_list_param_to_criteria("is_group", form.get_pax_type()),
                convert_list_param_to_criteria("dom_op_al_code", [*form.get_selected_competitors_list(), request.user.carrier]),
                {
                    "travel_year": int(year),
                    "travel_month": int(month),
                },
            ]
        )

        return [
            {"$match": match},
            {
                "$group": {
                    "_id": {
                        "dom_op_al_code": "$dom_op_al_code",
                        "travel_day_of_week": "$travel_day_of_week",
                        "local_dep_time": "$local_dep_time",
                        "next_dest": "$next_dest",
                        "equip": "$equip",
                    },
                    "pax": {
                        "$sum": "$pax",
                    },
                    "blended_rev": {
                        "$sum": "$blended_rev",
                    },
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "dom_op_al_code": "$_id.dom_op_al_code",
                    "travel_day_of_week": "$_id.travel_day_of_week",
                    "local_dep_time": "$_id.local_dep_time",
                    "next_dest": "$_id.next_dest",
                    "equip": "$_id.equip",
                    "pax": 1,
                    "blended_rev": {"$toInt": "$blended_rev"},
                }
            },
        ]

    def network_conictivity_map_pipeline(self, form: NetworkConictivityMap):
        match = merge_criterions(
            [
                convert_list_param_to_criteria("orig_code", form.get_orig_regions_list()),
                convert_list_param_to_criteria("dest_code", form.get_dest_regions_list()),
                convert_list_param_to_criteria("orig_code", form.get_orig_country_list()),
                convert_list_param_to_criteria("dest_code", form.get_dest_country_list()),
                convert_list_param_to_criteria("orig_code", form.get_orig_city_airports_list()),
                convert_list_param_to_criteria("dest_code", form.get_dest_city_airports_list()),
                convert_list_param_to_criteria("seg_class", form.get_cabins_list()),
                convert_list_param_to_criteria("country_of_sale", form.get_point_of_sales_list()),
                convert_list_param_to_criteria("distribution_channel", form.get_sales_channels_list()),
                convert_list_param_to_criteria("is_group", form.get_pax_type()),
                convert_list_param_to_criteria("dom_op_al_code", [*form.get_selected_competitors_list(), request.user.carrier]),
                {
                    "travel_year": int(form.selected_year.data),
                },
                {"is_ticket": True},
            ]
        )

        return [
            {"$match": match},
            {"$match": {"dom_op_al_code": {"$ne": "OTH"}}},
            {
                "$facet": {
                    "inbound": [
                        {"$match": {"prev_dest": {"$ne": "-"}}},
                        {
                            "$group": {
                                "_id": {
                                    "dom_op_al_code": "$dom_op_al_code",
                                    "prev_dest": "$prev_dest",
                                    "orig_code": "$orig_code",
                                },
                                "pax_sum": {"$sum": "$pax"},
                                "blended_rev": {"$sum": "$blended_rev"},
                            }
                        },
                        {
                            "$project": {
                                "_id": 0,
                                "dom_op_al_code": "$_id.dom_op_al_code",
                                "prev_dest": "$_id.prev_dest",
                                "orig_code": "$_id.orig_code",
                                "pax_sum": 1,
                                "blended_rev": 1,
                                "bound": "Inbound",
                                "path": {"$concat": ["$_id.orig_code", "-", "$_id.prev_dest"]},
                            }
                        },
                    ],
                    "outbound": [
                        {"$match": {"next_dest": {"$ne": "-"}}},
                        {
                            "$group": {
                                "_id": {
                                    "dom_op_al_code": "$dom_op_al_code",
                                    "next_dest": "$next_dest",
                                    "dest_code": "$dest_code",
                                },
                                "pax_sum": {"$sum": "$pax"},
                                "blended_rev": {"$sum": "$blended_rev"},
                            }
                        },
                        {"$match": {"pax_sum": {"$gte": 50}}},
                        {
                            "$project": {
                                "_id": 0,
                                "dom_op_al_code": "$_id.dom_op_al_code",
                                "next_dest": "$_id.next_dest",
                                "dest_code": "$_id.dest_code",
                                "pax_sum": 1,
                                "blended_rev": 1,
                                "bound": "Outbound",
                                "path": {"$concat": ["$_id.dest_code", "-", "$_id.next_dest"]},
                            }
                        },
                    ],
                }
            },
            {"$project": {"result": {"$concatArrays": ["$inbound", "$outbound"]}}},
            {"$unwind": {"path": "$result"}},
            {
                "$project": {
                    "pax_sum": "$result.pax_sum",
                    "blended_rev": {"$round": "$result.blended_rev"},
                    "dom_op_al_code": "$result.dom_op_al_code",
                    "bound": "$result.bound",
                    "path": "$result.path",
                }
            },
        ]
