from datetime import date

import pandas as pd
from flask import request

from airports.repository import AirportRepository
from base.constants import Constants
from base.helpers.permissions import has_access
from base.middlewares import attach_figure_id, attach_story_text
from base.service import BaseService
from dds.repository import DdsRepository
from market_share.forms import MsdMarketShareTrends
from msd_schedule.repository import MsdScheduleRepository
from utils.funcs import format_time_duration, table_to_text

from .builder import DdsBuilder
from .cos_breakdown.charts import Figure, Text
from .cos_breakdown.query import COSBreakDownFiguresQuery, COSBreakDownTableQuery
from .cos_breakdown.table import Table
from .figure import DdsFigure
from .forms import (
    MsdBaseProductOverviewForm,
    MsdGetBkgMix,
    MsdGetCabinMix,
    MsdGetCosBreakdown,
    MsdGetDistrMix,
    MsdGetProductMap,
    PosBreakDownTables,
    RangeSliderValues,
)
from .utils import duration_to_str, list_to_str, most_popular, return_days


class DdsService(BaseService):
    msd_schedule_repository = MsdScheduleRepository()
    airport_repository = AirportRepository()
    repository_class = DdsRepository
    builder_class = DdsBuilder
    figure_class = DdsFigure

    table_labels = [
        {"carrier": "CARR"},
        {"aircraft_type": "EQP"},
        {"orig_code": "ORG"},
        {"dest_code": "DES"},
        {"local_dep_time": "DEP"},
        {"travel_day_of_week": "FREQ"},
        {"duration": "TRV TIME"},
        {"stop_1": "STOP"},
        {"seg_class": "SERVICE"},
    ]

    STORY_TEXTS = {
        "get_dist_mix": {"main_card": {"title": "test", "content": "test"}},
        "get_cos_plots": {"main_card": {"title": "test", "content": "test"}},
        "get_cabin_mix": {"main_card": {"title": "test", "content": "test"}},
        "get_bkg_type_mix": {"main_card": {"title": "test", "content": "test"}},
        "get_product_matrix": {"main_card": {"title": "test", "content": "test"}},
    }

    def get_healthcheck(self):
        return "OK"

    def get_msd_carriers(self):
        return self.repository.get_msd_carriers()

    def get_msd_config(self):
        return self.repository.get_msd_config()

    @has_access("MSD", ["/product-overview"])
    def get_market_share_trends(self, form: MsdMarketShareTrends):
        list_ds = form.date_range_start.data.split("-")
        list_de = form.date_range_start.data.split("-")
        picked_dates_start = date(int(list_ds[0]), int(list_ds[1]), int(list_ds[2]))
        picked_dates_end = date(int(list_de[0]), int(list_de[1]), int(list_de[2]))
        competitors = form.get_selected_competitors_list()
        monthly_sums_pipeline = self.builder.get_market_shared_trends_monthly_sums_pipeline(form)
        share_trends_pipeline = self.builder.get_market_share_trends_pipeline(form)
        # create dataframes
        monthly_sums_df = self.lambda_df(self.aggregate(monthly_sums_pipeline))
        data_df = self.lambda_df(self.aggregate(share_trends_pipeline))
        market_share_trends_df = monthly_sums_df.merge(data_df, on=["travel_year", "travel_month"])

        # proccess data
        market_share_trends_df["mk_share"] = market_share_trends_df["pax"] / market_share_trends_df["pax_sum"]
        market_share_trends_df["travel_date"] = market_share_trends_df.apply(
            lambda x: date(x["travel_year?"], x["travel_month"], 1), axis=1
        )
        market_share_trends_df = market_share_trends_df[
            (market_share_trends_df["travel_date"] >= picked_dates_start)
            & (market_share_trends_df["travel_date"] <= picked_dates_end)
        ]
        market_share_trends_df["flag"] = market_share_trends_df["dom_op_al_code"].apply(
            lambda x: int(x in ["PK"] + competitors.split(","))
        )
        market_share_trends_df = market_share_trends_df.query("flag == 1").reset_index(drop=True)

    @has_access("MSD", ["/product-overview"])
    def get_product_map(self, form: MsdGetProductMap):
        # TODO handle multiple airports (the below will fail if REST client sends comma separated list of airports)
        # DENIZ - we need to know what should we return in case multiple airports are requested
        orig_city_airport = form.orig_city_airport.data
        dest_city_airport = form.dest_city_airport.data

        # get lat and long for both origion and destination codes
        pt1 = self.airport_repository.get_coordinates(orig_city_airport)
        pt2 = self.airport_repository.get_coordinates(dest_city_airport)

        if not pt1 or not pt2:
            return

        orig_lat, orig_long = pt1
        dest_lat, dest_long = pt2

        return self.figure.product_map_viz(
            orig_long,
            dest_long,
            orig_lat,
            dest_lat,
            orig_city_airport,
            dest_city_airport,
        )

    @has_access("MSD", ["/product-overview"])
    @attach_figure_id()
    def get_dist_mix(self, form: MsdGetDistrMix):
        selected_year, selected_month = form.get_selected_year_month()
        # commenting this parameter for now because UI currently doesn't have an option for it
        # agg_view = form.agg_view.data
        # FIXME - hardcoded agg_view, review this once its clear what to do with it
        selected_agg_view = form.agg_view.data
        pipeline = self.builder.get_dist_mix_pipeline(form)
        dist_channel_pies = self.lambda_df(self.aggregate(pipeline))
        if dist_channel_pies.empty:
            return {
                **self.empty_figure,
                "story_text": {"main_card": {"content": "", "title": "Graph Text"}},
            }
        fig = self.figure.dist_mix_viz(dist_channel_pies, int(selected_year), selected_agg_view)

        to_text = table_to_text(
            dist_channel_pies,
            [("pax", "sum")],
            group_cols=["travel_year"],
            input_col_name="distribution_channel",
        )

        fig["story_text"] = {"main_card": {"content": to_text, "title": "Graph Text"}}

        return fig

    @has_access("MSD", ["/product-overview"])
    def get_cos_tables(self, form: PosBreakDownTables):
        data = self._aggregte(COSBreakDownTableQuery(form).query)

        if data.empty:
            return {
                "pax_table": {"data": self.empty_figure, "labels": {}},
                "fare_table": {"data": self.empty_figure, "labels": {}},
            }

        table = Table(data, request.user.carrier)

        pax_table, fare_table = table.get()
        return {
            "pax_table": {"data": pax_table, "labels": table.pax_columns},
            "fare_table": {"data": fare_table, "labels": table.fare_columns},
        }

    @has_access("MSD", ["/product-overview"])
    @attach_figure_id(["fig_host", "fig_comp"])
    def get_cos_plots(self, form: MsdGetCosBreakdown):
        data = self._aggregte(COSBreakDownFiguresQuery(form).query)
        figure = Figure(request.user.carrier, data, form)
        host_figure = figure.render(request.user.carrier)
        competitor_figure = figure.render(form.competitor())
        return {"fig_comp": competitor_figure, "fig_host": host_figure, "story_text": Text(data, request.user.carrier).generate()}

    @has_access("MSD", ["/product-overview"])
    @attach_figure_id()
    def get_cabin_mix(self, form: MsdGetCabinMix):
        selected_year, _ = form.get_selected_year_month()

        # commenting this parameter for now because UI currently doesn't have an option for it
        # agg_view = form.agg_view.data
        selected_agg_view = form.agg_view.data

        df = self.lambda_df(self.aggregate(self.builder.get_cabin_mix(form)))
        if df.empty:
            return {"data": [], "layout": {}}

        fig = self.figure.cabin_mix_viz(df, selected_year, selected_agg_view)

        to_text = table_to_text(df, [("pax", "sum")], group_cols=["travel_year"], input_col_name="seg_class")

        fig["story_text"] = {"main_card": {"content": to_text, "title": "Graph Text"}}

        return fig

    @has_access("MSD", ["/product-overview"])
    @attach_figure_id()
    def get_bkg_type_mix(self, form: MsdGetBkgMix):
        selected_year, selected_month = form.get_selected_year_month()

        # commenting this parameter for now because UI currently doesn't have an option for it
        # agg_view = form.agg_view.data
        selected_agg_view = form.agg_view.data

        seg_rbkd_pie = self.lambda_df(self.aggregate(self.builder.get_bkg_mix(form)))

        if seg_rbkd_pie.empty:
            return {"data": [], "layout": {}}

        seg_rbkd_pie["is_group"] = seg_rbkd_pie["is_group"].astype(int)
        seg_rbkd_pie["is_group"] = seg_rbkd_pie["is_group"].map({0: "Individual", 1: "Group"})
        # FIXME - above fails (exception) if mongo query does not return any data!!!!
        fig = self.figure.bkg_mix_viz(seg_rbkd_pie, picked_year=selected_year, agg_type=selected_agg_view)

        to_text = table_to_text(
            seg_rbkd_pie,
            [("pax", "sum")],
            group_cols=["travel_year"],
            input_col_name="is_group",
        )

        fig["story_text"] = {"main_card": {"content": to_text, "title": "Graph Text"}}

        return fig

    @has_access("MSD", ["/product-overview"])
    @attach_story_text(STORY_TEXTS["get_product_matrix"])
    def get_product_matrix(self, form: MsdBaseProductOverviewForm):
        pipeline = self.builder.get_product_matrix_pipeline(form)
        df = self.lambda_df(self.msd_schedule_repository.get_list(pipeline))
        if df.empty:
            return {"table": self.empty_figure}

        # #convert local-dep-time to string-time formated value (2050 -> 20:50)
        df["local_dep_time"] = df["local_dep_time"].apply(lambda val: format_time_duration(val))

        # NOTE i'm only getting the first carrier needs to be handled
        df["carrier"] = df["carrier"].apply(lambda x: x[0])
        # df['aircraft_type'] = df.apply(lambda x: '{}'.format(x['equip']), axis=1)

        df["aircraft_type"] = df.apply(lambda x: ",".join(x["equip"]), axis=1)

        # convert week days from numbers 2 -> string format like - T - - - - -
        # for specific flight
        result_df = (
            df.groupby(["orig_code", "dest_code", "carrier", "local_dep_time"])
            .agg({"travel_day_of_week": return_days})
            .reset_index()
        )

        result_df = result_df.merge(
            df[
                [
                    "orig_code",
                    "dest_code",
                    "carrier",
                    "cabins",
                    "local_dep_time",
                    "aircraft_type",
                    "duration",
                    "stops",
                ]
            ]
            .dropna(subset=["aircraft_type"])
            .groupby(["orig_code", "dest_code", "carrier", "local_dep_time"])
            .agg(
                {
                    "duration": duration_to_str,
                    "aircraft_type": most_popular,
                    "stops": most_popular,
                    "cabins": list_to_str,
                }
            )
            .reset_index(),
            on=["orig_code", "dest_code", "carrier", "local_dep_time"],
        )
        # the frontend guy expects columns with spesific names
        result_df.rename(columns={"stops": "stop_1", "cabins": "seg_class"}, inplace=True)

        return {
            "table": {
                "data": self.__get_carriers_color(result_df),
                "labels": self.table_labels,
            },
        }

    def __get_carriers_color(self, df):
        """get colors for each carrier code"""
        table_data = []
        colors = self.get_carrier_color_map()
        for _, row in df.drop_duplicates(
            subset=[
                "carrier",
                "aircraft_type",
                "local_dep_time",
                "travel_day_of_week",
                "duration",
                "stop_1",
            ]
        ).iterrows():
            curr = {}
            for elem in self.table_labels:
                k = list(elem.keys())[0]
                if k == "carrier":
                    curr_inner = {
                        "color": colors.get(row[k]) or "#9c00ed",
                        "value": row[k],
                    }
                    curr[k] = curr_inner
                else:
                    curr[k] = row[k]
            table_data.append(curr)
        return table_data

    def get_silder_range_values(self, form: RangeSliderValues):
        """get rande values for sliders along with initial values"""
        today = date.today()
        values = self.__handle_range()
        start_idx, end_idx = self.__handle_init_range_values(values)
        return {
            "default_date": str(date(today.year, today.month, 1)),
            "values": values,
            "start_idx": start_idx,
            "end_idx": end_idx,
        }

    def handle_daily_range(self, start_date: date, end_date: date):
        """get daily dates range"""
        return [dt.strftime("%Y-%m-%d") for dt in pd.date_range(str(start_date), str(end_date)).tolist()]

    def handle_monthly_range(self, start_date: date, end_date: date):
        """
        get monthly dates range
        if user is looking historial or hisroical-forward get the first day of each month
        if user is looking only forward start from current day
        """
        time_direction = request.args.get("time_direction", "historical-forward")
        today = date.today()
        to_be_considred_date = today if time_direction == "forward" else date(today.year, today.month, 1)
        return [
            dt.strftime("%Y-%m-%d")
            for dt in pd.date_range(str(start_date), str(end_date)).tolist()
            if dt.day == 1 or dt == to_be_considred_date
        ]

    def handle_yearly_range(self, start_date: date, end_date: date):
        """get yearly (only years) dates range"""
        return sorted(list(set([dt.strftime("%Y") for dt in pd.date_range(str(start_date), str(end_date)).tolist()])))

    def get_start_date(self, agg_type):
        """get inital start date"""
        today = date.today()
        time_direction = request.args.get("time_direction", "historical-forward")
        # in case of yearly agg_type we care only about year
        if agg_type == Constants.AGG_VIEW_YEARLY:
            return str(today.year)
        # if monthly and forward we start with the first day of current month and year
        if agg_type == Constants.AGG_VIEW_MONTHLY and time_direction != "forward":
            return str(date(today.year, today.month, 1))
        # default : we start with current date
        return str(today)

    def __handle_range(self):
        """this function generates date values based on agg_type"""
        agg_type = request.args.get("agg_type", Constants.AGG_VIEW_YEARLY)
        start_date, end_date = self.__get_range_dates()
        range_method = getattr(self, f"handle_{agg_type}_range")
        return range_method(start_date, end_date)

    def __get_range_dates(self):
        """get start_date, end_date for a range based on agg_type"""
        today = date.today()
        agg_type = request.args.get("agg_type", Constants.AGG_VIEW_YEARLY)
        time_direction = request.args.get("time_direction", "historical-forward")
        prev_date = date(today.year - 1, 1, 1)
        next_date = date(today.year + 1, 1, 1) if Constants.AGG_VIEW_YEARLY == agg_type else date(today.year + 1, 12, 31)

        if time_direction == "historical":
            return prev_date, today
        if time_direction == "forward":
            return today, next_date
        return prev_date, next_date

    def __handle_init_range_values(self, dts_range):
        """get index of start_date, end_date based on agg_type and time_direction"""
        agg_type = request.args.get("agg_type", Constants.AGG_VIEW_YEARLY)
        time_direction = request.args.get("time_direction", "historical-forward")

        if time_direction == "forward" and agg_type != Constants.AGG_VIEW_YEARLY:
            return 0, 5

        if time_direction == "historical" and agg_type != Constants.AGG_VIEW_YEARLY:
            return len(dts_range) - 6, len(dts_range) - 1

        start_index = dts_range.index(self.get_start_date(agg_type))

        # in case of yearly agg_type we end_date_index is always -1
        if agg_type == Constants.AGG_VIEW_YEARLY:
            return start_index, -1

        return start_index, start_index + 5
