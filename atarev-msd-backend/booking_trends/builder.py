from typing import Any, Dict

from flask import request

from base.constants import Constants
from base.mongo_utils import convert_list_param_to_criteria, merge_criterions
from booking_trends.forms import BookingCurve, BookingTrends
from utils.funcs import get_date_as_int


class FareBookingBuilder:
    def booking_trends_pipeline(self, form: BookingTrends):
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
            ]
        )

        if request.args.get("agg_type") == Constants.AGG_VIEW_MONTHLY:
            group = {"travel_year": "$travel_year", "travel_month": "$travel_month"}
            project: Dict[str, Any] = {"travel_year": "$_id.travel_year", "travel_month": "$_id.travel_month"}

        else:
            group = {"travel_date": "$travel_date"}
            project = {
                "travel_date": "$_id.travel_date",
                "travel_year": {"$toInt": {"$substr": [{"$toString": "$_id.travel_date"}, 0, 4]}},
                "travel_month": {"$toInt": {"$substr": [{"$toString": "$_id.travel_date"}, 4, 2]}},
            }

        return [
            {"$match": match},
            {"$match": {"$and": [{"dom_op_al_code": {"$in": form.get_graph_carriers()}}, {"dom_op_al_code": {"$ne": "OTH"}}]}},
            {"$group": {"_id": {"dom_op_al_code": "$dom_op_al_code", **group}, "pax": {"$sum": "$pax"}}},
            {"$project": {"_id": 0, "pax": 1, "dom_op_al_code": "$_id.dom_op_al_code", **project}},
        ]

    def holiday_pipeline(self, form: BookingTrends):
        countries = request.args.get("holiday_countries", "").split(",")
        selected_holidays = form.get_selected_holidays()
        match: Dict[str, Any] = {
            "$and": [
                {"start_date": {"$gte": get_date_as_int(form.date_range_start.data)}},
                {"start_date": {"$lte": get_date_as_int(form.date_range_end.data)}},
            ],
        }

        if selected_holidays:
            match["holiday_idx"] = {"$in": selected_holidays}

        if countries:
            match["country_name"] = {"$in": countries}

        return [{"$match": match}]

    def booking_curve_pipeline(self, form: BookingCurve):
        year, month, _ = form.get_selected_year_month()
        agg_type = request.args.get("agg_type", "overall")

        groupby = {
            "_id": {"dom_op_al_code": "$dom_op_al_code", "days_sold_prior_to_travel": "$days_sold_prior_to_travel"},
            "pax": {"$sum": "$pax"},
            "days_sold_prior_to_travel": {"$push": "days_sold_prior_to_travel"},
        }
        project = {
            "pax": 1,
            "dom_op_al_code": "$_id.dom_op_al_code",
            "days_sold_prior_to_travel": "$_id.days_sold_prior_to_travel",
        }

        if agg_type == "day-of-week":
            groupby["_id"]["travel_day_of_week"] = "$travel_day_of_week"
            project["travel_day_of_week"] = "$_id.travel_day_of_week"

        if agg_type == "day-of-week-time":
            groupby["_id"]["travel_day_of_week"] = "$travel_day_of_week"
            groupby["_id"]["local_dep_time"] = "$local_dep_time"
            project["travel_day_of_week"] = "$_id.travel_day_of_week"
            project["local_dep_time"] = "$_id.local_dep_time"

        conditions = [
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

        if request.args.get("dtd"):
            conditions.append({"days_sold_prior_to_travel": {"$lte": int(request.args["dtd"])}})

        return [
            {"$match": merge_criterions(conditions)},
            {"$match": {"$and": [{"dom_op_al_code": {"$ne": "OTH"}}, {"dom_op_al_code": {"$in": form.get_graph_carriers()}}]}},
            {"$group": groupby},
            {"$project": {"_id": 0, **project}},
            {"$sort": {"days_sold_prior_to_travel": -1}},
        ]
