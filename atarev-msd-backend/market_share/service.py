from datetime import date

import pandas as pd
from flask import request

from base.constants import Constants
from base.helpers.permissions import has_access
from base.middlewares import attach_figure_id, attach_story_text, cache
from base.service import BaseService
from base.utils import add_missing_dates, return_date_parts_as_int
from dds.repository import DdsRepository
from market_share.builder import MarketShareBuilder
from market_share.figure import MarketShareFigure
from utils.funcs import table_to_text


class MarketShareService(BaseService):
    builder_class = MarketShareBuilder
    repository_class = DdsRepository
    figure_class = MarketShareFigure
    # NOTE : values here are hard-coded
    market_share_trends_response = {
        "today_date": date.today().strftime("%d/%m/%Y"),
        "summary_metrics": {
            "totalMarketSize": {"total_capacity": 2000, "total_size": 1600},
            "hostMarketSize": {"total_capacity": 1000, "total_size": 800},
            "compMarketSize": {"total_capacity": 1000, "total_size": 800},
            "totalMarketTrends": {"total_capacity": 1000, "total_size": 800},
        },
    }

    STORY_TEXTS = {
        "get_market_share_trends": {
            "main_card": {"title": "test", "content": "test"},
            "share_trends": {"title": "test", "content": "test"},
            "fare_trends": {"title": "test", "content": "test"},
        },
        "get_market_share_avg_fare": {"main_card": {"title": "test", "content": "test"}},
    }

    @has_access("MSD", ["/market-share"])
    @attach_figure_id(["fig"])
    @cache()
    def get_market_share(self, form, typ: str):
        pipeline = self.builder.market_share_pipeline(form, typ)
        df = self._aggregte(pipeline)

        if df.empty:
            return {
                "fig_shareTrends": self.empty_figure,
                "fig_fareTrends": self.empty_figure,
                **self.market_share_trends_response,
            }

        monthly_sum = (
            df.groupby(["travel_year", "travel_month"], as_index=False)
            .agg(monthly_sum=("pax", "sum"))
            .merge(
                df.groupby("travel_year")
                .agg({"blended_fare": "mean"})
                .rename(columns={"blended_fare": "year_industry_fare"})
                .reset_index(),
                on="travel_year",
            )
        )

        market_sum = df.groupby(["travel_year", "travel_month", "dom_op_al_code"], as_index=False).agg(
            {"pax": "sum", "blended_fare": "mean"}
        )

        monthly_sum = monthly_sum.merge(market_sum)
        monthly_sum["mkt_share"] = monthly_sum["pax"] / monthly_sum["monthly_sum"]
        monthly_sum["blended_fare"] = monthly_sum.blended_fare * form.get_currency_exchange_rate()
        monthly_sum["currency"] = monthly_sum.shape[0] * [form.get_currency() or "USD"]

        if typ == Constants.MARKET_SHARE_TRENDS:
            return self.get_market_share_trends(monthly_sum)
        return self.get_market_share_avg_fare(monthly_sum)

    def get_market_share_trends(self, monthly_sum_df: pd.DataFrame):
        market_share = add_missing_dates(
            monthly_sum_df,
            columns=["mkt_share"],
            start_date=request.args.get("date_range_start"),
            end_date=request.args.get("date_range_end"),
            typ=Constants.AGG_VIEW_MONTHLY,
        )

        fare_trend_df = add_missing_dates(
            monthly_sum_df,
            columns=["blended_fare", "currency"],
            start_date=request.args.get("date_range_start"),
            end_date=request.args.get("date_range_end"),
            typ=Constants.AGG_VIEW_MONTHLY,
        )

        fig_market_share = self.figure.market_share_viz(market_share)
        fig_trend_fares = self.figure.fare_trends_viz(fare_trend_df)

        to_text = table_to_text(fare_trend_df, [("blended_fare", "mean")])

        response = {
            "fig_shareTrends": fig_market_share,
            "fig_fareTrends": fig_trend_fares,
            "story_text": {"main_card": {"content": to_text, "title": "Graph Text"}},
            **self.market_share_trends_response,
        }

        return response

    @attach_story_text(STORY_TEXTS["get_market_share_avg_fare"])
    def get_market_share_avg_fare(self, monthly_sum_df: pd.DataFrame):
        monthly_sum_df["travel_date"] = monthly_sum_df.apply(
            lambda x: return_date_parts_as_int(x["travel_year"], x["travel_month"], 1),
            axis=1,
        )

        year, month = request.args.get("selected_yearmonth").split("-")
        fig = self.figure.mshare_afare_viz(
            monthly_sum_df[(monthly_sum_df["travel_year"] == int(year)) & (monthly_sum_df["travel_month"] == int(month))]
        )

        host_data = self.get_mkt_share_and_avg_blended_fare_for_a_host(request.user.carrier, monthly_sum_df)
        host_data = self.get_fare_and_market_share_variance(host_data)
        comp_data = self.get_mkt_share_and_avg_blended_fare_for_a_host(request.args.get("main_competitor"), monthly_sum_df)
        comp_data = self.get_fare_and_market_share_variance(comp_data)

        response = {
            "fig": fig,
            "host_metrics": {
                "pickedYearmo_avgFare": f"{host_data['avg_fare_curr']}",
                "pickedYearmo_mktShare": self.refacor_pickedYearmo_mktShare(host_data["mkt_share_curr"]),
                "pickedYearmo_avgFareVar": host_data["avg_fare_variance"],
                "pickedYearmo_mktShareVar": host_data["avg_fare_ly_ind_fare_var"],
            },
            "comp1_metrics": {
                "pickedYearmo_avgFare": comp_data["avg_fare_curr"],
                "pickedYearmo_mktShare": self.refacor_pickedYearmo_mktShare(comp_data["mkt_share_curr"]),
                "pickedYearmo_avgFareVar": comp_data["avg_fare_variance"],
                "pickedYearmo_mktShareVar": comp_data["avg_fare_ly_ind_fare_var"],
            },
            "df_all": str(monthly_sum_df.values),
        }

        return response

    def refacor_pickedYearmo_mktShare(self, data):
        return f"{round(data * 100)}%" if isinstance(data, (int, float)) else data

    def get_mkt_share_and_avg_blended_fare_for_a_host(self, host_code: str, df: pd.DataFrame):
        """
        return current fare,prev fare,current market share and prev market share for
        a specific host code as a dict
        """
        df = df.sort_values("travel_date", ascending=False)
        selected_month = int(request.args.get("selected_yearmonth").split("-")[1])
        selected_year = int(request.args.get("selected_yearmonth").split("-")[0])

        curr_year, curr_month = (selected_year, selected_month)
        prev_year, prev_month = (selected_year - 1, selected_month)

        host_df = df[df["dom_op_al_code"] == host_code]

        if (
            host_df.empty
            or curr_year not in host_df.travel_year.unique().tolist()
            or curr_month not in host_df.travel_month.unique().tolist()
        ):
            return {
                "avg_fare_curr": "-",
                "avg_fare_prev": "-",
                "mkt_share_curr": "-",
                "mkt_share_prev": "-",
            }

        data_dict = {
            "avg_fare_curr": round(
                host_df[(host_df["travel_year"] == curr_year) & (host_df["travel_month"] == curr_month)].blended_fare.iloc[0]
            ),
            "mkt_share_curr": host_df[
                (host_df["travel_year"] == curr_year) & (host_df["travel_month"] == curr_month)
            ].mkt_share.iloc[0],
            "avg_fare_prev": "-",
            "mkt_share_prev": "-",
            "last_year_industry_fare": "-",
        }

        if prev_year in host_df.travel_year.unique().tolist() and prev_month in host_df.travel_month.unique().tolist():
            ind_fare_ly = host_df.query(f"travel_year == {prev_year}").iloc[0].year_industry_fare
            data_dict["avg_fare_prev"] = round(
                host_df[(host_df["travel_year"] == prev_year) & (host_df["travel_month"] == prev_month)].blended_fare.iloc[0]
            )
            data_dict["mkt_share_prev"] = round(
                host_df[(host_df["travel_year"] == prev_year) & (host_df["travel_month"] == prev_month)].mkt_share.iloc[0]
            )
            data_dict["last_year_industry_fare"] = ind_fare_ly

        return data_dict

    def get_fare_and_market_share_variance(self, data_dict: dict):
        if (
            data_dict["avg_fare_curr"] == "-"
            or data_dict["avg_fare_prev"] == "-"
            or data_dict["mkt_share_curr"] == "-"
            or data_dict["mkt_share_prev"] == "-"
        ):
            return {
                **data_dict,
                "avg_fare_variance": "-",
                "mkt_share_variance": ",",
                "avg_fare_ly_ind_fare_var": "-",
            }
        data_dict["avg_fare_variance"] = (
            str(round((data_dict["avg_fare_curr"] - data_dict["avg_fare_prev"]) / data_dict["avg_fare_prev"] * 100)) + "%"
        )
        data_dict["mkt_share_variance"] = str(round((data_dict["mkt_share_curr"] - data_dict["mkt_share_prev"]))) + "%"

        data_dict["avg_fare_ly_ind_fare_var"] = (
            str(
                round(
                    (data_dict["avg_fare_curr"] - data_dict["last_year_industry_fare"])
                    / data_dict["last_year_industry_fare"]
                    * 100
                )
            )
            + "%"
        )
        return data_dict
