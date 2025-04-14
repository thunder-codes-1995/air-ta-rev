from datetime import date, timedelta
from typing import Union

from flask import request

from agency_analysis.forms import AgencyGraph, AgencyQuadrant, AgencyTable
from base.mongo_utils import convert_list_param_to_criteria, merge_criterions


class AgencyBuilder:
    YEARLY_COMP_TYPE = "year"
    MONTHLY_COMP_TYPE = "month"

    def agency_pipeline(self, form: Union[AgencyTable, AgencyQuadrant]):
        if request.args.get("comp_type") == self.MONTHLY_COMP_TYPE:
            year, month = form.get_selected_year_month()
        else:
            year = form.selected_yearmonth.data
        conditions = [
            convert_list_param_to_criteria("orig_code", form.get_orig_city_airports_list()),
            convert_list_param_to_criteria("dest_code", form.get_dest_city_airports_list()),
            convert_list_param_to_criteria("country_of_sale", form.get_point_of_sales_list()),
            convert_list_param_to_criteria("is_group", form.get_pax_type()),
            convert_list_param_to_criteria("distribution_channel", form.get_sales_channels_list()),
            convert_list_param_to_criteria("seg_class", form.get_cabins_list()),
            convert_list_param_to_criteria("dom_op_al_code", [*form.get_selected_competitors_list(), request.user.carrier]),
        ]

        groupby = {
            "agency_id": "$agency_id",
            "agency_name": "$agency_name",
            "agency_country": "$agency_country",
            "dom_op_al_code": "$dom_op_al_code",
            "travel_year": "$travel_year",
        }

        project = {
            "_id": 0,
            "agency_id": "$_id.agency_id",
            "agency_name": "$_id.agency_name",
            "agency_country": "$_id.agency_country",
            "dom_op_al_code": "$_id.dom_op_al_code",
            "travel_year": "$_id.travel_year",
            "pax": 1,
            "blended_rev": 1,
            "blended_fare": 1,
        }

        if request.args.get("comp_type") == self.MONTHLY_COMP_TYPE:
            prev_period = date(int(year), int(month), 1) - timedelta(weeks=4)
            conditions.append(
                {
                    "$or": [
                        {"travel_year": int(year), "travel_month": int(month)},
                        {"travel_year": prev_period.year, "travel_month": prev_period.month},
                    ]
                }
            )

            groupby["travel_month"] = "$travel_month"
            project["travel_month"] = "$_id.travel_month"

        else:
            conditions.append({"$or": [{"travel_year": int(year)}, {"travel_year": int(year) - 1}]})

        match = merge_criterions(conditions)
        return [
            {"$match": match},
            {
                "$group": {
                    "_id": groupby,
                    "pax": {"$sum": "$pax"},
                    "blended_rev": {"$sum": "$blended_rev"},
                    "blended_fare": {"$avg": "$blended_fare"},
                }
            },
            {"$project": project},
        ]

    def agency_graph_pipeline(self, form: AgencyGraph):
        if request.args.get("comp_type") == self.MONTHLY_COMP_TYPE:
            year, month = form.get_selected_year_month()
        else:
            year = form.selected_yearmonth.data
        date_match = {"travel_year": int(year)}

        if request.args.get("comp_type") == self.MONTHLY_COMP_TYPE:
            date_match["travel_month"] = int(month)

        match = [
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
            convert_list_param_to_criteria("dom_op_al_code", [*form.get_selected_competitors_list(), request.user.carrier]),
            date_match,
        ]

        if request.args.get("selected_agency").strip():
            match.append(
                convert_list_param_to_criteria(
                    "agency_id", [int(agncy_id) for agncy_id in request.args.get("selected_agency").split(",")]
                )
            )
        return [
            {"$match": merge_criterions(match)},
            {
                "$addFields": {
                    "is_weekend": {"$cond": [{"$in": ["$travel_day_of_week", [5, 6, 7]]}, True, False]},
                }
            },
            {
                "$addFields": {
                    "num_days_bins": {
                        "$switch": {
                            "branches": [
                                {
                                    "case": {
                                        "$and": [
                                            {"$gte": ["$days_sold_prior_to_travel", 0]},
                                            {"$lte": ["$days_sold_prior_to_travel", 7]},
                                        ]
                                    },
                                    "then": "0-7",
                                },
                                {
                                    "case": {
                                        "$and": [
                                            {"$gte": ["$days_sold_prior_to_travel", 8]},
                                            {"$lte": ["$days_sold_prior_to_travel", 30]},
                                        ]
                                    },
                                    "then": "08-30",
                                },
                                {
                                    "case": {
                                        "$and": [
                                            {"$gte": ["$days_sold_prior_to_travel", 31]},
                                            {"$lte": ["$days_sold_prior_to_travel", 60]},
                                        ]
                                    },
                                    "then": "31-60",
                                },
                                {
                                    "case": {
                                        "$and": [
                                            {"$gte": ["$days_sold_prior_to_travel", 61]},
                                            {"$lte": ["$days_sold_prior_to_travel", 90]},
                                        ]
                                    },
                                    "then": "61-90",
                                },
                            ],
                            "default": "90+",
                        },
                    }
                }
            },
            {
                "$addFields": {
                    "pax_conv": {
                        "$switch": {
                            "branches": [
                                {"case": {"pax": 1}, "then": "1"},
                                {
                                    "case": {
                                        "$and": [
                                            {"$gte": ["pax", 2]},
                                            {"$lte": ["pax", 9]},
                                        ]
                                    },
                                    "then": "2-9",
                                },
                            ],
                            "default": "9+",
                        },
                    }
                }
            },
            {
                "$project": {
                    "ticket_type": 1,
                    "travel_day_of_week": 1,
                    "dom_op_al_code": 1,
                    "agency_id": 1,
                    "agency_name": 1,
                    "agency_country": 1,
                    "agency_continent": 1,
                    "num_days_bins": 1,
                    "is_weekend": 1,
                    "seg_class": 1,
                    "rbkd": 1,
                    "pax": 1,
                    "blended_fare": 1,
                    "blended_rev": 1,
                    "travel_year": 1,
                    "travel_month": 1,
                    "travel_date": 1,
                    "pax_conv": 1,
                }
            },
        ]
