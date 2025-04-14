from datetime import date

import numpy as np
import pandas as pd
import statsmodels.api as sm
from flask import request

from base.constants import Constants
from base.entities.currency import Currency
from base.helpers.permissions import has_access
from base.middlewares import attach_figure_id, attach_story_text, cache
from base.service import BaseService
from base.utils import add_missing_dates
from dds.forms import MsdBaseProductOverviewForm
from dds.repository import DdsRepository
from fare_revenue.builder import FareRevenueBuilder
from fare_revenue.figure import FareRevenueFigure
from fare_revenue.forms import (
    FareRevenueClassMix,
    FareRevenueTrends,
    MSDBookingVsAverageFares,
    MSDFareRevenueDowRevenue,
    MSDRbdElastic,
)
from fare_revenue.utils import class_mix
from utils.funcs import table_to_text


class FareRevenueService(BaseService):
    builder_class = FareRevenueBuilder
    repository_class = DdsRepository
    figure_class = FareRevenueFigure

    STORY_TEXTS = {
        "get_fare_trends": {"main_card": {"title": "test", "content": "test"}},
        "get_fare_booking_histograms": {"main_card": {"title": "test", "content": "test"}},
        "get_fare_dow_revenue": {"main_card": {"title": "test", "content": "test"}},
        "get_msd_rbd_elastic": {"main_card": {"title": "test", "content": "test"}},
        "get_fare_revenue_class_mix": {"main_card": {"title": "test", "content": "test"}},
        "get_fare_revenue_trends": {"main_card": {"title": "test", "content": "test"}},
    }

    @has_access("MSD", ["/fare-revenue"])
    @attach_figure_id(["fig"])
    def get_fare_trends(self, form: MsdBaseProductOverviewForm):
        today = date.today().strftime("%d/%m/%Y")

        # get pipline and get data based on pipeline
        pipeline = self.builder.get_fare_trends_pipeline(form)
        df = self.lambda_df(self.repository.aggregate(pipeline))

        if df.empty:
            return {"fig": {"data": [], "layout": {}}, "today_date": today}

        agg_type = request.args.get("agg_type")
        fares_df = self.get_date_fare_trends_df(df)

        fares_df.convert_currency(value_col="blended_fare", convert_to=form.get_currency())
        to_text = table_to_text(fares_df, [("blended_fare", "mean")])

        fig = self.figure.fare_trends_viz(fares_df, agg_type)

        return {
            "fig": fig,
            "today_date": today,
            "story_text": {"main_card": {"content": to_text, "title": "Graph Text"}},
        }

    def get_date_fare_trends_df(self, df: pd.DataFrame):
        """converts date columns from string to datetime object"""
        if request.args.get("agg_type") == Constants.AGG_VIEW_MONTHLY:
            df["travel_date"] = df.apply(lambda x: date(x["travel_year"], x["travel_month"], 1), axis=1)
        else:
            df["travel_date"] = df["travel_date"].apply(lambda x: f"{str(x)[0:4]}-{str(x)[4:6]}-{str(x)[6:]}")
            df["travel_date"] = pd.DatetimeIndex(df["travel_date"]).normalize()
            df["travel_date"] = df["travel_date"].apply(lambda x: x.date())
        return df

    @has_access("MSD", ["/fare-revenue"])
    @cache()
    @attach_figure_id(["fig_host", "fig_comp"])
    def get_fare_booking_histograms(self, form: MSDBookingVsAverageFares):
        pipeline = self.builder.get_fare_booking_histograms_pipeline(form)
        df = self._aggregte(pipeline)

        if df.empty:
            return {"fig_host": self.empty_figure, "fig_comp": self.empty_figure}

        host_df = df[df.dom_op_al_code == request.user.carrier]
        comp_df = df[df.dom_op_al_code == request.args.get("main_competitor", "")]
        host_figure = self.figure.fare_histogram_viz(host_df, request.user.carrier) if not host_df.empty else self.empty_figure
        comp_figure = (
            self.figure.fare_histogram_viz(comp_df, request.args.get("main_competitor", ""))
            if not comp_df.empty
            else self.empty_figure
        )

        host_text = "Host " + table_to_text(
            host_df,
            [("blended_rev", "sum")],
            group_cols=["travel_year", "travel_day_of_week"],
            input_col_name="histogram",
        )

        comp_text = "Competitor " + table_to_text(
            comp_df,
            [("blended_rev", "sum")],
            group_cols=["travel_year", "travel_day_of_week"],
            input_col_name="histogram",
        )

        to_text = "\n".join([host_text, comp_text])

        resp = {
            "fig_host": host_figure,
            "fig_comp": comp_figure,
            "story_text": {"main_card": {"content": to_text, "title": "Graph Text"}},
        }

        return resp

    @has_access("MSD", ["/fare-revenue"])
    @cache()
    @attach_figure_id(["fig"])
    def get_fare_dow_revenue(self, form: MSDFareRevenueDowRevenue):
        pipeline = self.builder.get_fare_dow_revenue_pipeline(form)
        df = self.lambda_df(self.repository.aggregate(pipeline))

        if df.empty:
            return {"fig": self.empty_figure}

        df["travel_date"] = df.apply(lambda x: date(x["travel_year"], x["travel_month"], 1), axis=1)
        df.convert_currency(value_col="blended_rev", convert_to=form.get_currency())
        fig = self.figure.fare_dow_revenue_viz(df)

        to_text = table_to_text(
            df,
            [("blended_rev", "sum")],
            group_cols=["travel_year", "travel_day_of_week"],
            input_col_name="travel_day_of_week",
        )

        return {
            "fig": fig,
            "currency": ",".join(list(Currency(df.currency.unique().tolist()).symbol.values())),
            "story_text": {"main_card": {"content": to_text, "title": "Graph Text"}},
        }

    @has_access("MSD", ["/fare-revenue"])
    @cache()
    @attach_figure_id(["host", "comp"])
    def get_msd_rbd_elastic(self, form: MSDRbdElastic):
        pipeline = self.builder.get_msd_rbd_ealstic_pipeline(form)
        df = self._aggregte(pipeline)
        if df.empty:
            return {
                "host": self.empty_figure,
                "comp": self.empty_figure,
                "legend_scale": [],
            }
        df = self.__get_rbd_elastic_perios_df(df)
        elasticity_df = self.get_rbd_elastic_df(df)
        main_al = request.user.carrier
        competitor_al = request.args.get("main_competitor", "")
        host_fig = self.figure.rbd_elastic_viz(elasticity_df, main_al, main_al, competitor_al)
        comp_fig = self.figure.rbd_elastic_viz(elasticity_df, competitor_al, main_al, competitor_al)

        to_text = "This heatmap shows RBD price elasticities across three periods within the selected date range. Each value represents the number of passengers gained/lost with a $1 increase in price."

        resp = {
            "legend_scale": sorted(np.round([e for row in elasticity_df.values for e in row], 2)),
            "host": host_fig,
            "comp": comp_fig,
            "story_text": {"main_card": {"content": to_text, "title": "Graph Text"}},
        }

        return resp

    def __get_rbd_elastic_perios_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        for each travel year, month define to which period a record belongs
        one year consists of 3 periods (4 months for each)
        """
        PERIODS_COUNT = 3
        periods_df = (
            df.drop_duplicates(["travel_year", "travel_month"])
            .sort_values(["travel_year", "travel_month"], ascending=True)
            .reset_index(drop=True)
        )

        periods = np.ceil(periods_df.shape[0] / PERIODS_COUNT)
        periods_df["period"] = np.floor(periods_df.index / periods)
        return df.merge(
            periods_df[["travel_year", "travel_month", "period"]],
            on=["travel_year", "travel_month"],
        )

    def __get_peds(self, df: pd.DataFrame) -> pd.DataFrame:
        """for every rbkd,period,carrier combination :
        - categorize blended_fare and get upper bound price
        - fit upper price (based on OLS model) estimation will serve as elasticity
        - get the "price_upper mean" for each rbkd,upper_price combination
        """
        peds = []
        BINS_COUNT = 20

        for g, g_df in df.groupby(["rbkd", "period", "dom_op_al_code"]):
            # categorize blended_fare into categories
            g_df["price_bins"] = pd.cut(g_df["blended_fare"].round(decimals=0), bins=BINS_COUNT)
            # get the upper bound (upper price) for each cateogry
            g_df["upper_price"] = g_df["price_bins"].apply(lambda val: val.right).astype(int)
            grouped = g_df.groupby(["rbkd", "upper_price"], as_index=False).agg({"pax": "sum"})
            params = self.__fit_price(grouped)
            peds.append((g[0], g[1], g[2], params["upper_price"], grouped.upper_price.mean()))

        peds_df = pd.DataFrame(peds, columns=["RBD", "Yearmo", "Carrier", "Price Elasticity", "price_mean"])
        peds_df.sort_values(by="price_mean", inplace=True)
        return peds_df

    def get_rbd_elastic_df(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        peds_df = self.__get_peds(df)
        ped_agg = (
            peds_df.groupby(["Yearmo", "RBD", "Carrier"])
            .agg({"Price Elasticity": "sum"})
            .unstack(level=["Yearmo"])
            .fillna(0)
            .round(2)
        )
        return ped_agg

    def __fit_price(self, df: pd.DataFrame):
        x = sm.add_constant(df["upper_price"])
        model = sm.OLS(df["pax"], x)
        return model.fit().params

    @has_access("MSD", ["/fare-revenue"])
    @cache()
    @attach_story_text(STORY_TEXTS["get_fare_revenue_class_mix"])
    @attach_figure_id(["fig_host", "fig_comp"])
    def get_fare_revenue_class_mix(self, form: FareRevenueClassMix):
        year, month, _ = form.get_date_parts(request.args.get("selected_yearmonth"))
        pipelineـsums = self.builder.get_fare_revenue_class_mix_pipeline(form, "sums")
        pipelineـmix = self.builder.get_fare_revenue_class_mix_pipeline(form, "mix")
        pipelineـavg = self.builder.get_fare_revenue_class_mix_pipeline(form, "avg")

        df_sums = self.lambda_df(self.repository.aggregate(pipelineـsums))
        df_mix = self.lambda_df(self.repository.aggregate(pipelineـmix))
        df_avg = self.lambda_df(self.repository.aggregate(pipelineـavg))

        if df_sums.empty or df_mix.empty or df_avg.empty:
            return {
                "fig_host": {"data": [], "layout": {}},
                "fig_comp": {"data": [], "layout": {}},
            }

        g1 = df_sums.groupby(["dom_op_al_code", "travel_day_of_week"], as_index=False).agg(overall_sum=("pax", "sum"))
        g2 = df_sums.groupby(
            ["dom_op_al_code", "travel_day_of_week", "seg_class", "rbkd"],
            as_index=False,
        ).sum()
        df_sums = g1.merge(g2)
        df_sums["percentage"] = df_sums.apply(lambda d: d["pax"] / d["overall_sum"], axis=1)
        df_sums["day_text"] = df_sums["travel_day_of_week"].map(Constants.IDX2DAY)

        g1 = df_mix.groupby(["dom_op_al_code", "seg_class"], as_index=False).agg(pax_sum=("pax", "sum"))
        g2 = df_mix.groupby(["dom_op_al_code", "seg_class", "rbkd"], as_index=False).agg({"pax": "sum", "blended_fare": "mean"})
        df_mix = g1.merge(g2)
        df_mix = df_mix.drop_duplicates(["dom_op_al_code", "seg_class", "rbkd"])
        df_avg, yearmo_colors = class_mix(df_avg, year, month)

        figs = self.figure.class_mix_viz(
            df_sums,
            df_mix,
            df_avg,
            yearmo_colors,
            request.args.get("main_competitor"),
            (year, month),
        )

        resp = {"fig_host": figs[0], "fig_comp": figs[1]}

        return resp

    @has_access("MSD", ["/fare-revenue"])
    @cache()
    @attach_figure_id(["fig"])
    def get_fare_revenue_trends(self, form: FareRevenueTrends):
        s_year, s_month, _ = form.get_date_parts(request.args.get("date_range_start"))
        e_year, e_month, _ = form.get_date_parts(request.args.get("date_range_end"))

        pipeline = self.builder.get_fare_revenue_trends_pipline(form)
        df = self._aggregte(pipeline)
        if df.empty:
            return {"fig": self.empty_figure}

        df["travel_date"] = df.apply(lambda x: date(x["travel_year"], x["travel_month"], 1), axis=1)
        df.convert_currency(value_col="blended_rev", convert_to=form.get_currency())

        fares_df = add_missing_dates(
            df,
            columns=["blended_rev", "currency"],
            start_date=f"{s_year}-{s_month}-01",
            end_date=f"{e_year}-{e_month}-01",
        )

        fig = self.figure.get_fare_revenue_trends_viz(fares_df)

        to_text = table_to_text(fares_df, [("blended_rev", "sum")])

        return {
            "fig": fig,
            "currency": ",".join(list(Currency(df.currency.unique().tolist()).symbol.values())),
            "story_text": {"main_card": {"content": to_text, "title": "Graph Text"}},
        }
