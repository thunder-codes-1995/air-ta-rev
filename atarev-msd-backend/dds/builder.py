from flask import request

from base.constants import Constants
from base.mongo_utils import convert_list_param_to_criteria, merge_criterions
from dds.forms import BaseMSDFilterParams, MsdGetBkgMix, MsdGetCabinMix, MsdGetDistrMix
from market_share.forms import MsdMarketShareTrends


class DdsBuilder:
    def get_base_match(self, form: BaseMSDFilterParams):
        match = merge_criterions(
            [
                convert_list_param_to_criteria("orig_code", form.get_orig_city_airports_list()),
                convert_list_param_to_criteria("dest_code", form.get_dest_city_airports_list()),
                convert_list_param_to_criteria("distribution_channel", form.get_sales_channels_list()),
                convert_list_param_to_criteria("seg_class", form.get_class_list()),
                convert_list_param_to_criteria("country_of_sale", form.get_point_of_sales_list()),
                convert_list_param_to_criteria("dom_op_al_code", [*form.get_selected_competitors_list(), request.user.carrier]),
                convert_list_param_to_criteria("travel_year", int(form.selected_yearmonth.data.split("-")[0])),
            ]
        )

        if form.pax_type.data == Constants.PAX_TYPE_INDIVIDUAL:
            match.append(convert_list_param_to_criteria("is_group", False))

        if form.pax_type.data == Constants.PAX_TYPE_GROUP:
            match.append(convert_list_param_to_criteria("is_group", True))

        return match

        # common match object - creates mongo query condition based on parameters from the client

    def get_base_match_pgsdemo(self, form: BaseMSDFilterParams, ignore_parameters=[]):
        origin_airports = form.get_orig_city_airports_list()
        destination_airports = form.get_dest_city_airports_list()
        classes_list = form.get_class_list()
        distribution_channels = form.get_sales_channels_list()
        pos = form.get_point_of_sales_list()

        # if 'pax_type' == GRP it's a group, if 'IND' it's individual pax, 'All' - both
        if form.pax_type.data == Constants.PAX_TYPE_INDIVIDUAL:
            is_group = [False]
        elif form.pax_type.data == Constants.PAX_TYPE_GROUP:
            is_group = [True]
        else:
            is_group = [True, False]

        main_competitor = form.get_main_competitor()
        selected_competitors = form.get_selected_competitors_list()
        filt_carrier_codes = list(set([request.user.carrier] + selected_competitors + [main_competitor]))
        filt_carrier_codes = list(filter(lambda code: code != "OTH", filt_carrier_codes))

        match = merge_criterions(
            [
                convert_list_param_to_criteria("orig_code", origin_airports),
                convert_list_param_to_criteria("dest_code", destination_airports),
                convert_list_param_to_criteria("distribution_channel", distribution_channels),
                convert_list_param_to_criteria("seg_class", classes_list),
                convert_list_param_to_criteria("country_of_sale", pos),
                convert_list_param_to_criteria("is_group", is_group),
                convert_list_param_to_criteria("dom_op_al_code", filt_carrier_codes),
                convert_list_param_to_criteria("travel_year", int(form.selected_yearmonth.data.split("-")[0])),
            ]
        )
        for ignore_parameter in ignore_parameters:
            match.pop(ignore_parameter, None)

        return match

    def get_market_shared_trends_monthly_sums_pipeline(self, form: MsdMarketShareTrends):
        match = self.get_base_match(form)

        return [
            {"$match": match},
            {
                "$group": {
                    "_id": {
                        "travel_year": "$travel_year",
                        "travel_month": "$travel_month",
                    },
                    "pax_sum": {"$sum": "$pax"},
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "travel_year": "$_id.travel_year",
                    "travel_month": "$_id.travel_month",
                    "pax_sum": 1,
                }
            },
        ]

    def get_market_share_trends_pipeline(self, form: BaseMSDFilterParams):
        match = self.get_base_match(form)

        return [
            {"$match": match},
            {
                "$group": {
                    "_id": {"travel_year": "$travel_year", "travel_month": "$travel_month", "dom_op_al_code": "$dom_op_al_code"},
                    "blended_fare": {"$avg": "$blended_fare"},
                    "pax": {"$sum": "$pax"},
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "travel_year": "$_id.travel_year",
                    "travel_month": "$_id.travel_month",
                    "dom_op_al_code": "$_id.dom_op_al_code",
                    "pax": 1,
                    "blended_fare": 1,
                }
            },
        ]

    def get_dist_mix_pipeline(self, form: MsdGetDistrMix):
        match = self.get_base_match_pgsdemo(form, ["seg_class"])

        group_by = {
            "_id": {
                "dom_op_al_code": "$dom_op_al_code",
                "travel_year": "$travel_year",
                "distribution_channel": "$distribution_channel",
            },
            "pax": {"$sum": "$pax"},
        }

        project = {
            "_id": 0,
            "dom_op_al_code": "$_id.dom_op_al_code",
            "travel_year": "$_id.travel_year",
            "distribution_channel": "$_id.distribution_channel",
            "pax": 1,
        }

        if form.agg_view.data == Constants.AGG_VIEW_MONTHLY:
            group_by["_id"]["travel_month"] = "$travel_month"
            project["travel_month"] = "$_id.travel_month"

        return [{"$match": match}, {"$group": group_by}, {"$project": project}]

    def get_cabin_mix(self, form: MsdGetCabinMix):
        match = self.get_base_match_pgsdemo(form, ["distribution_channel", "country_of_sale"])

        if form.agg_view.data == Constants.AGG_VIEW_YEARLY:
            return [
                {"$match": match},
                {
                    "$group": {
                        "_id": {"dom_op_al_code": "$dom_op_al_code", "travel_year": "$travel_year", "seg_class": "$seg_class"},
                        "pax": {"$sum": "$pax"},
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "dom_op_al_code": "$_id.dom_op_al_code",
                        "travel_year": "$_id.travel_year",
                        "seg_class": "$_id.seg_class",
                        "pax": 1,
                    }
                },
            ]
        else:
            return [
                {"$match": match},
                {
                    "$group": {
                        "_id": {
                            "dom_op_al_code": "$dom_op_al_code",
                            "travel_year": "$travel_year",
                            "seg_class": "$seg_class",
                            "travel_month": "$travel_month",
                        },
                        "pax": {"$sum": "$pax"},
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "dom_op_al_code": "$_id.dom_op_al_code",
                        "travel_year": "$_id.travel_year",
                        "travel_month": "$_id.travel_month",
                        "seg_class": "$_id.seg_class",
                        "pax": 1,
                    }
                },
            ]

    def get_bkg_mix(self, form: MsdGetBkgMix):
        match = self.get_base_match_pgsdemo(form, ["seg_class"])

        if form.agg_view.data == Constants.AGG_VIEW_YEARLY or not form.agg_view.data:
            return [
                {"$match": match},
                {
                    "$group": {
                        "_id": {
                            "dom_op_al_code": "$dom_op_al_code",
                            "travel_year": "$travel_year",
                            "is_group": "$is_group",
                        },
                        "pax": {"$sum": "$pax"},
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "dom_op_al_code": "$_id.dom_op_al_code",
                        "travel_year": "$_id.travel_year",
                        "is_group": "$_id.is_group",
                        "pax": 1,
                    }
                },
            ]
        else:
            return [
                {"$match": match},
                {
                    "$group": {
                        "_id": {
                            "dom_op_al_code": "$dom_op_al_code",
                            "travel_year": "$travel_year",
                            "travel_month": "$travel_month",
                            "is_group": "$is_group",
                        },
                        "pax": {"$sum": "$pax"},
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "dom_op_al_code": "$_id.dom_op_al_code",
                        "travel_year": "$_id.travel_year",
                        "travel_month": "$_id.travel_month",
                        "is_group": "$_id.is_group",
                        "pax": 1,
                    }
                },
            ]

    def get_product_matrix_pipeline(self, form: BaseMSDFilterParams):
        year_month = form.get_selected_year_month()
        year = int(year_month[0])
        month = int(year_month[1])

        match = merge_criterions(
            [
                convert_list_param_to_criteria("orig_code", form.get_orig_regions_list()),
                convert_list_param_to_criteria("dest_code", form.get_dest_regions_list()),
                convert_list_param_to_criteria("orig_code", form.get_orig_country_list()),
                convert_list_param_to_criteria("dest_code", form.get_dest_country_list()),
                convert_list_param_to_criteria("orig_code", form.get_orig_city_airports_list()),
                convert_list_param_to_criteria("dest_code", form.get_dest_city_airports_list()),
                convert_list_param_to_criteria("travel_year", year),
                convert_list_param_to_criteria("travel_month", month),
                convert_list_param_to_criteria("cabins", form.get_cabins_list()),
                convert_list_param_to_criteria("carrier", [*form.get_selected_competitors_list(), request.user.carrier]),
            ]
        )
        return [{"$match": match}]
