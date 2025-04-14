from calendar import monthrange

from flask import request

from base.constants import Constants
from base.mongo_utils import (
    merge_criterions,
    convert_list_param_to_criteria
)


class MarketShareBuilder:

    
    def market_share_pipeline(self,form, typ):

        if typ == Constants.MARKET_SHARE_TRENDS:
            s_year, s_month, _ = form.get_date_parts(request.args.get('date_range_start'))
            e_year, e_month, _ = form.get_date_parts(request.args.get('date_range_end'))
            e_days = monthrange(e_year, e_month)[1]
            s_month = f"0{s_month}" if s_month < 10 else f"{s_month}"
            e_month = f"0{e_month}" if e_month < 10 else f"{e_month}"
            e_days = f"0{e_days}" if e_days < 10 else f"{e_days}"

        if typ == Constants.MARKET_SHARE_AVG:
            selected_month = int(request.args.get('selected_yearmonth').split('-')[1])
            selected_year = int(request.args.get('selected_yearmonth').split('-')[0])

        conditions = [
            convert_list_param_to_criteria('orig_code', form.get_orig_regions_list()),
            convert_list_param_to_criteria('dest_code', form.get_dest_regions_list()),
            convert_list_param_to_criteria('orig_code', form.get_orig_country_list()),
            convert_list_param_to_criteria('dest_code', form.get_dest_country_list()),
            convert_list_param_to_criteria('orig_code', form.get_orig_city_airports_list()),
            convert_list_param_to_criteria('dest_code', form.get_dest_city_airports_list()),
            convert_list_param_to_criteria('seg_class', form.get_cabins_list()),
            convert_list_param_to_criteria('country_of_sale', form.get_point_of_sales_list()),
            convert_list_param_to_criteria('distribution_channel', form.get_sales_channels_list()),
            convert_list_param_to_criteria('is_group', form.get_pax_type()),
        ]

        # if is_demo_mode():
        #    conditions.append({"is_ticket" : False})

        if typ == Constants.MARKET_SHARE_TRENDS:
            conditions.append({
                "$and": [
                    {'travel_date': {"$gte": int(f"{s_year}{s_month}01")}},
                    {'travel_date': {"$lte": int(f"{e_year}{e_month}{e_days}")}},
                ]
            })

        if typ == Constants.MARKET_SHARE_AVG:
            conditions.append({
                "$and": [
                    {'travel_month': {"$in": [selected_month]}},
                    {'travel_year': {"$in": [selected_year, selected_year - 1]}}
                ]
            })

        match = merge_criterions(conditions)

        return [
            {
                "$match": match,
            },
            {
                "$match": {
                    'dom_op_al_code': {"$ne": "OTH"}
                }

            },

            {
                "$project": {
                    "dom_op_al_code": 1,
                    "travel_year": 1,
                    'travel_month': 1,
                    'pax': 1,
                    'blended_fare': 1,
                    'flag': {"$cond": [{"$in": ["$dom_op_al_code", [*form.get_graph_carriers()]]}, 1, 0]},

                }
            },
            {
                "$match": {
                    "flag": 1
                }
            },
            {
                "$sort": {
                    "travel_year": -1
                }
            }

        ]
