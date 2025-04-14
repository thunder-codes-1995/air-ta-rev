from calendar import monthrange

from flask import request

from base.constants import Constants
from base.mongo_utils import convert_list_param_to_criteria, merge_criterions
from fare_revenue.forms import (FareRevenueClassMix, FareRevenueTrends,
                                MSDBookingVsAverageFares,
                                MSDFareRevenueDowRevenue,
                                MSDProductRevenueFareTrends, MSDRbdElastic)


class FareRevenueBuilder:
    def get_fare_trends_pipeline(self, form: MSDProductRevenueFareTrends):
        s_year, s_month, _ = form.get_date_parts(request.args.get("date_range_start"))
        e_year, e_month, _ = form.get_date_parts(request.args.get("date_range_end"))
        e_days = monthrange(e_year, e_month)[1]
        s_month = f"0{s_month}" if s_month < 10 else f"{s_month}"
        e_month = f"0{e_month}" if e_month < 10 else f"{e_month}"
        e_days = f"0{e_days}" if e_days < 10 else f"{e_days}"

        match = merge_criterions(
            [
                convert_list_param_to_criteria("orig_code", form.get_orig_city_airports_list()),
                convert_list_param_to_criteria("dest_code", form.get_dest_city_airports_list()),
                convert_list_param_to_criteria("seg_class", form.get_cabins_list()),
                convert_list_param_to_criteria("country_of_sale", form.get_point_of_sales_list()),
                convert_list_param_to_criteria("distribution_channel", form.get_sales_channels_list()),
                convert_list_param_to_criteria("is_group", form.get_pax_type()),
                convert_list_param_to_criteria(
                    "dom_op_al_code",
                    [
                        *list(
                            filter(
                                lambda carrier_code: carrier_code in form.get_graph_carriers(),
                                form.get_selected_competitors_list(),
                            )
                            if "All" not in form.get_graph_carriers()
                            else [*form.get_selected_competitors_list()]
                        ),
                        request.user.carrier,
                    ],
                ),
                # {"is_ticket" : False if request.args.get('agg_type') == Constants.AGG_VIEW_MONTHLY else True },
                {
                    "$and": [
                        {"travel_date": {"$gte": int(f"{s_year}{s_month}01")}},
                        {"travel_date": {"$lte": int(f"{e_year}{e_month}{e_days}")}},
                    ]
                },
            ]
        )

        if request.args.get("agg_type") == Constants.AGG_VIEW_MONTHLY:
            groupby = {"dom_op_al_code": "$dom_op_al_code", "travel_month": "$travel_month", "travel_year": "$travel_year"}
            project = {"travel_month": "$_id.travel_month", "travel_year": "$_id.travel_year"}

        else:
            groupby = {"dom_op_al_code": "$dom_op_al_code", "travel_date": "$travel_date"}
            project = {
                "travel_date": "$_id.travel_date",
                "travel_year": {"$toInt": {"$substr": [{"$toString": "$_id.travel_date"}, 0, 4]}},
                "travel_month": {"$toInt": {"$substr": [{"$toString": "$_id.travel_date"}, 4, 2]}},
            }

        project = {
            "_id": 0,
            "blended_fare": 1,
            "dom_op_al_code": "$_id.dom_op_al_code",
            **project,
        }

        return [
            {
                "$match": match,
            },
            {"$match": {"dom_op_al_code": {"$ne": "OTH"}}},
            {
                "$group": {
                    "_id": {**groupby, "dom_op_al_code": "$dom_op_al_code"},
                    "blended_fare": {"$avg": "$blended_fare"},
                }
            },
            {"$project": project},
        ]

    def get_fare_booking_histograms_pipeline(self, form: MSDBookingVsAverageFares):
        year, month = form.get_selected_year_month()
        match = merge_criterions(
            [
                convert_list_param_to_criteria("orig_code", form.get_orig_regions_list()),
                convert_list_param_to_criteria("dest_code", form.get_dest_regions_list()),
                convert_list_param_to_criteria("orig_code", form.get_orig_country_list()),
                convert_list_param_to_criteria("dest_code", form.get_dest_country_list()),
                convert_list_param_to_criteria("orig_code", form.get_orig_city_airports_list()),
                convert_list_param_to_criteria("dest_code", form.get_dest_city_airports_list()),
                convert_list_param_to_criteria("country_of_sale", form.get_point_of_sales_list()),
                convert_list_param_to_criteria("is_group", form.get_pax_type()),
                convert_list_param_to_criteria("distribution_channel", form.get_sales_channels_list()),
                convert_list_param_to_criteria("seg_class", form.get_cabins_list()),
                {"travel_year": int(year)},
                {"travel_month": int(month)},
            ]
        )

        return [{"$match": match}, {"$project": {"dom_op_al_code": 1, "blended_fare": 1, "pax": 1}}]

    def get_fare_dow_revenue_pipeline(self, form: MSDFareRevenueDowRevenue):
        year, month = form.get_selected_year_month()
        match = merge_criterions(
            [
                convert_list_param_to_criteria("orig_code", form.get_orig_regions_list()),
                convert_list_param_to_criteria("dest_code", form.get_dest_regions_list()),
                convert_list_param_to_criteria("orig_code", form.get_orig_country_list()),
                convert_list_param_to_criteria("dest_code", form.get_dest_country_list()),
                convert_list_param_to_criteria("orig_code", form.get_orig_city_airports_list()),
                convert_list_param_to_criteria("dest_code", form.get_dest_city_airports_list()),
                convert_list_param_to_criteria("distribution_channel", form.get_sales_channels_list()),
                convert_list_param_to_criteria("country_of_sale", form.get_point_of_sales_list()),
                convert_list_param_to_criteria("is_group", form.get_pax_type()),
                convert_list_param_to_criteria("seg_class", form.get_cabins_list()),
                convert_list_param_to_criteria("dom_op_al_code", [request.user.carrier, *form.get_graph_carriers()]),
                {"travel_year": int(year)},
                {"travel_month": int(month)},
            ]
        )

        return [
            {"$match": match},
            {"$match": {"dom_op_al_code": {"$ne": "OTH"}}},
            {
                "$group": {
                    "_id": {
                        "dom_op_al_code": "$dom_op_al_code",
                        "travel_year": "$travel_year",
                        "travel_month": "$travel_month",
                        "travel_day_of_week": "$travel_day_of_week",
                    },
                    "blended_rev": {"$sum": "$blended_rev"},
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "dom_op_al_code": "$_id.dom_op_al_code",
                    "travel_year": "$_id.travel_year",
                    "travel_month": "$_id.travel_month",
                    "travel_day_of_week": "$_id.travel_day_of_week",
                    "blended_rev": 1,
                }
            },
        ]

    def get_msd_rbd_ealstic_pipeline(self, form: MSDRbdElastic):
        s_year, s_month, _ = form.get_date_parts(request.args.get("date_range_start"))
        e_year, e_month, _ = form.get_date_parts(request.args.get("date_range_end"))
        e_days = monthrange(e_year, e_month)[1]
        e_days = str(e_days) if e_days > 9 else f"0{e_days}"
        s_month = f"0{s_month}" if s_month < 10 else f"{s_month}"
        e_month = f"0{e_month}" if e_month < 10 else f"{e_month}"

        match = merge_criterions(
            [
                convert_list_param_to_criteria("orig_code", form.get_orig_regions_list()),
                convert_list_param_to_criteria("dest_code", form.get_dest_regions_list()),
                convert_list_param_to_criteria("orig_code", form.get_orig_country_list()),
                convert_list_param_to_criteria("dest_code", form.get_dest_country_list()),
                convert_list_param_to_criteria("orig_code", form.get_orig_city_airports_list()),
                convert_list_param_to_criteria("dest_code", form.get_dest_city_airports_list()),
                convert_list_param_to_criteria("dom_op_al_code", [request.user.carrier, request.args.get("main_competitor", "")]),
                convert_list_param_to_criteria("country_of_sale", form.get_point_of_sales_list()),
                convert_list_param_to_criteria("is_group", form.get_pax_type()),
                convert_list_param_to_criteria("distribution_channel", form.get_sales_channels_list()),
                convert_list_param_to_criteria("seg_class", form.get_cabins_list()),
                {
                    "$and": [
                        {"travel_date": {"$gte": int(f"{s_year}{s_month}01")}},
                        {"travel_date": {"$lte": int(f"{e_year}{e_month}{e_days}")}},
                    ]
                },
            ]
        )

        return [
            {"$match": match},
            {
                "$project": {
                    "dom_op_al_code": 1,
                    "travel_year": 1,
                    "travel_month": 1,
                    "rbkd": 1,
                    "pax": 1,
                    "blended_fare": 1,
                    "dom_op_al_code": 1,
                }
            },
        ]

    def get_fare_revenue_class_mix_pipeline(self, form: FareRevenueClassMix, typ: str):
        year, month, _ = form.get_selected_year_month()
        match = merge_criterions(
            [
                convert_list_param_to_criteria("orig_code", form.get_orig_regions_list()),
                convert_list_param_to_criteria("dest_code", form.get_dest_regions_list()),
                convert_list_param_to_criteria("orig_code", form.get_orig_country_list()),
                convert_list_param_to_criteria("dest_code", form.get_dest_country_list()),
                convert_list_param_to_criteria("orig_code", form.get_orig_city_airports_list()),
                convert_list_param_to_criteria("dest_code", form.get_dest_city_airports_list()),
                convert_list_param_to_criteria("country_of_sale", form.get_point_of_sales_list()),
                convert_list_param_to_criteria("is_group", form.get_pax_type()),
                convert_list_param_to_criteria("dom_op_al_code", [*form.get_selected_competitors_list(), request.user.carrier]),
                convert_list_param_to_criteria("distribution_channel", form.get_sales_channels_list()),
                convert_list_param_to_criteria("seg_class", form.get_cabins_list()),
                {"travel_year": int(year)},
                {"travel_month": int(month)},
            ]
        )

        if typ == "sums":
            return [
                {"$match": match},
                {"$project": {"dom_op_al_code": 1, "travel_day_of_week": 1, "seg_class": 1, "rbkd": 1, "pax": 1}},
            ]

        if typ == "mix":
            return [
                {"$match": match},
                {
                    "$project": {
                        "dom_op_al_code": 1,
                        "blended_fare": 1,
                        "seg_class": 1,
                        "rbkd": {"$cond": [{"$not": ["$rbkd"]}, "-", "$rbkd"]},
                        "pax": 1,
                    }
                },
            ]

        return [
            {"$match": match},
            {
                "$group": {
                    "_id": {"dom_op_al_code": "$dom_op_al_code", "seg_class": "$seg_class", "rbkd": "$rbkd"},
                    "blended_fare": {"$avg": "$blended_fare"},
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "dom_op_al_code": "$_id.dom_op_al_code",
                    "blended_fare": 1,
                    "seg_class": "$_id.seg_class",
                    "rbkd": {"$cond": [{"$not": ["$_id.rbkd"]}, "-", "$_id.rbkd"]},
                }
            },
            {"$sort": {"blended_fare": 1}},
        ]

    def get_fare_revenue_trends_pipline(self, form: FareRevenueTrends):
        s_year, s_month, _ = form.get_date_parts(request.args.get("date_range_start"))
        e_year, e_month, _ = form.get_date_parts(request.args.get("date_range_end"))
        e_days = monthrange(e_year, e_month)[1]
        e_days = str(e_days) if e_days > 9 else f"0{e_days}"
        s_month = f"0{s_month}" if s_month < 10 else f"{s_month}"
        e_month = f"0{e_month}" if e_month < 10 else f"{e_month}"

        match = merge_criterions(
            [
                convert_list_param_to_criteria("orig_code", form.get_orig_regions_list()),
                convert_list_param_to_criteria("dest_code", form.get_dest_regions_list()),
                convert_list_param_to_criteria("orig_code", form.get_orig_country_list()),
                convert_list_param_to_criteria("dest_code", form.get_dest_country_list()),
                convert_list_param_to_criteria("orig_code", form.get_orig_city_airports_list()),
                convert_list_param_to_criteria("dest_code", form.get_dest_city_airports_list()),
                convert_list_param_to_criteria("country_of_sale", form.get_point_of_sales_list()),
                convert_list_param_to_criteria("is_group", form.get_pax_type()),
                convert_list_param_to_criteria("dom_op_al_code", [request.user.carrier, *form.get_graph_carriers()]),
                convert_list_param_to_criteria("distribution_channel", form.get_sales_channels_list()),
                convert_list_param_to_criteria("seg_class", form.get_cabins_list()),
                {
                    "$and": [
                        {"travel_date": {"$gte": int(f"{s_year}{s_month}01")}},
                        {"travel_date": {"$lte": int(f"{e_year}{e_month}{e_days}")}},
                    ]
                },
            ]
        )

        return [
            {"$match": match},
            {"$match": {"dom_op_al_code": {"$ne": "OTH"}}},
            {
                "$group": {
                    "_id": {
                        "dom_op_al_code": "$dom_op_al_code",
                        "travel_year": "$travel_year",
                        "travel_month": "$travel_month",
                        # "travel_day_of_week" : "$travel_day_of_week"
                    },
                    "blended_rev": {"$sum": "$blended_rev"},
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "dom_op_al_code": "$_id.dom_op_al_code",
                    "travel_year": "$_id.travel_year",
                    "travel_month": "$_id.travel_month",
                    # "travel_day_of_week" :"$_id.travel_day_of_week",
                    "blended_rev": 1,
                }
            },
        ]
