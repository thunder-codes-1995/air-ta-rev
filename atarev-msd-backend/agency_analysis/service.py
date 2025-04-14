from datetime import date, timedelta
from typing import List, Union

import numpy as np
import pandas as pd
from flask import request

from agency_analysis.builder import AgencyBuilder
from agency_analysis.figure import AgencyFigure
from agency_analysis.forms import AgencyGraph, AgencyQuadrant, AgencyTable
from base.entities.currency import Currency
from base.helpers.permissions import has_access
from base.middlewares import attach_figure_id, attach_story_text, cache
from base.service import BaseService
from dds.repository import DdsRepository

from .agency_comparison_table.service import ComparisonTable, Growth, MarketShareByCarrier


class AgencyService(BaseService):
    builder_class = AgencyBuilder
    repository_class = DdsRepository
    figure_class = AgencyFigure

    YEARLY_COMP_TYPE = "year"
    MONTHLY_COMP_TYPE = "month"
    SHARE_AGG_TYPE = "share"
    VOLUME_AGG_TYPE = "volume"
    AGENCY_COL_CONV = {
        "agency_id": "Agency ID",
        "agency_name": "Agency Name",
        "agency_country": "Agency Country",
        "this_year_market_size": "Market Size (TY)",
        "last_year_market_size": "Market Size (LY)",
        "market_growth": "Market Growth",
        "host_vol_this_year": "Host Vol (TY)",
        "host_vol_last_year": "Host Vol (LY)",
        "host_vol_growth": "Host Vol Growth",
        "host_share_this_year": "Host Share (TY)",
        "host_share_last_year": "Host Share (LY)",
        "host_share_growth": "Host Share Growth",
        "host_share_gap": "Host Share Gap",
        "host_rev_this_year": "Host Rev (TY)",
        "host_rev_last_year": "Host Rev (LY)",
        "host_rev_var": "Host Rev Var %",
        "host_avgfare_this_year": "Host Avg Fare (TY)",
        "host_avgfare_last_year": "Host Avg Fare (LY)",
        "host_avgfare_var": "Host Avg Fare Var %",
        "comp_1": "Comp#1",
        "comp1_vol_this_year": "Vol (TY)",
        "comp1_vol_last_year": "Vol (LY)",
        "comp1_vol_growth": "Vol Growth",
        "comp1_share_this_year": "Share (TY)",
        "comp1_share_last_year": "Share (LY)",
        "comp1_share_growth": "Share Growth",
        "comp1_share_gap": "Share Gap",
        "comp1_rev_this_year": "Rev (TY)",
        "comp1_rev_last_year": "Rev (LY)",
        "comp1_rev_var": "Rev Var %",
        "comp1_avgfare_this_year": "Avg Fare (TY)",
        "comp1_avgfare_last_year": "Avg Fare (LY)",
        "comp1_avgfare_var": "Avg Fare Var %",
        "comp_2": "Comp#2",
        "comp2_vol_this_year": "Vol (TY)",
        "comp2_vol_last_year": "Vol (LY)",
        "comp2_vol_growth": "Vol Growth",
        "comp2_share_this_year": "Share (TY)",
        "comp2_share_last_year": "Share (LY)",
        "comp2_share_growth": "Share Growth",
        "comp2_share_gap": "Share Gap",
        "comp2_rev_this_year": "Rev (TY)",
        "comp2_rev_last_year": "Rev (LY)",
        "comp2_rev_var": "Rev Var %",
        "comp2_avgfare_this_year": "Avg Fare (TY)",
        "comp2_avgfare_last_year": "Avg Fare (LY)",
        "comp2_avgfare_var": "Avg Fare Var %",
    }

    STORY_TEXTS = {
        "get_agency_table": {"main_card": {"title": "test", "content": "test"}},
        "get_agency_quadrant": {"main_card": {"title": "test", "content": "test"}},
        "get_agency_graphs": {
            "main_card": {"title": "test", "content": "test"},
            "bookings": {"title": "test", "content": "test"},
            "fare_revenue": {"title": "test", "content": "test"},
            "ticket_type": {"title": "test", "content": "test"},
            "days_to_departure": {"title": "test", "content": "test"},
            "dow_revenue": {"title": "test", "content": "test"},
            "passenger_volume": {"title": "test", "content": "test"},
            "class_rbd": {"title": "test", "content": "test"},
        },
    }

    @has_access("MSD", ["/agency-analysis"])
    @cache()
    @attach_story_text(STORY_TEXTS["get_agency_table"])
    def get_agency_table(self, form: AgencyTable):
        periods = self.get_periods()
        pipeline = self.builder.agency_pipeline(form)
        df = self._aggregte(pipeline)

        if df.empty:
            return {"labels": [], "data": []}
        # market_share_map = MarketShareByCarrier(df, request.user.carrier).get()
        # carriers = tuple(market_share_map.keys())
        # growth_df = Growth(df, form.get_period(), form.growth_type(), request.user.carrier, carriers).get()
        # return ComparisonTable(growth_df, form.growth_type()).get()

        current_period_df = self.get_period_df(df, periods["current_year"], periods["current_month"])
        prev_period_df = self.get_period_df(df, periods["prev_year"], periods["prev_month"])

        # get market share for each host carrier
        mkt_share = current_period_df.groupby(["dom_op_al_code"]).agg({"pax": "sum"}) / df.pax.sum()
        comps = mkt_share.reset_index().rename(columns={"pax": "share"}).sort_values(by="share", ascending=False)

        # label carriers with Host ,Comp#<number of competitor>)
        hosts = [e for e in comps.dom_op_al_code.tolist() if e != request.user.carrier][:1]
        host_map = {e: "Comp#{}".format(i + 1) for i, e in enumerate(hosts)}
        host_map[request.user.carrier] = "Host"
        comps["al_type"] = comps["dom_op_al_code"].map(host_map)

        current_period_df = current_period_df.merge(comps, on="dom_op_al_code")
        prev_period_df = prev_period_df.merge(comps, on="dom_op_al_code")

        if (
            current_period_df.empty
            or prev_period_df.empty
            or request.user.carrier not in current_period_df.dom_op_al_code.unique().tolist()
            or request.user.carrier not in prev_period_df.dom_op_al_code.unique().tolist()
        ):
            return {"labels": [], "data": []}

        current_period_df = self.get_market_host_share(current_period_df, True, ["Host", "Comp#1"])
        prev_period_df = self.get_market_host_share(prev_period_df, False, ["Host", "Comp#1"])

        if current_period_df.empty or prev_period_df.empty:
            return {"labels": [], "data": []}

        # get comparison_df (compare between current and prev period data)
        comparison_df = self.get_comparison_df(hosts, df, current_period_df, prev_period_df)

        # return data of comparison as table (dict) assosicated with labeles
        host_map_rev = {v: k for k, v in host_map.items()}
        labels, table = self.get_comparison_as_table(hosts, host_map_rev, comparison_df)

        response = {"labels": labels, "data": table}
        return response

    def get_period_df(self, df: pd.DataFrame, year: int, month: Union[int, None] = None) -> pd.DataFrame:
        """pass in a df and get sub-df based on period (year, month if exists)"""
        if month:
            return df[(df["travel_year"] == year) & (df["travel_month"] == month)]
        return df[df["travel_year"] == year]

    def get_periods(self):
        """get current and prev (year,month)"""

        if request.args.get("comp_type") == "month":
            year, month = request.args.get("selected_yearmonth").split("-")
        else:
            year, month = request.args.get("selected_yearmonth"), 1

        comp_type = request.args.get("comp_type")
        prev_period = date(int(year), int(month), 1) - timedelta(weeks=4)

        if comp_type == self.MONTHLY_COMP_TYPE:
            return {
                "current_year": int(year),
                "prev_year": prev_period.year,
                "current_month": int(month),
                "prev_month": prev_period.month,
            }

        return {
            "current_year": int(year),
            "prev_year": int(year) - 1,
            "current_month": None,
            "prev_month": None,
        }

    def get_market_host_share(
        self,
        df: pd.DataFrame,
        current_period: bool,
        market: List[str] = [],
        get_rev: bool = False,
    ) -> pd.DataFrame:
        """Get market share for a single carrier (market = []) or
        for multi carriers (market=[....])"""
        prefix = "this_year" if current_period else "last_year"
        agg_cols = {"pax": "sum"}
        volume_col_names = {"pax": f"{prefix}_volume"}
        market_col_names = {"pax": f"{prefix}_market_size"}

        # current_period_df = self.get_market_host_share(current_period_df, True, ["Host", "Comp#1"])
        # prev_period_df = self.get_market_host_share(prev_period_df, False, ["Host", "Comp#1"])

        if get_rev:
            agg_cols.update({"blended_rev": "sum"})
            volume_col_names.update({"blended_rev": f"{prefix}_rev"})
            market_col_names.update({"blended_rev": f"{prefix}_rev"})

        if market:
            df = df.query(f"al_type in {market}")

        result = (
            df.groupby(["agency_id", "agency_name", "al_type"])
            .agg(agg_cols)
            .fillna(0)
            .rename(columns=volume_col_names)
            .unstack("al_type")
            .merge(
                df.groupby(["agency_id", "agency_name"]).agg(agg_cols).fillna(0).rename(columns=market_col_names),
                left_index=True,
                right_index=True,
            )
        )

        if get_rev:
            result[f"host_rev_{prefix}"] = result[(f"{prefix}_rev", "Host")].sum()
            result[f"comp1_rev_{prefix}"] = result[(f"{prefix}_rev", "Comp#1")].sum()

        return result

    def get_comparison_df(
        self,
        hosts: List[str],
        init_df: pd.DataFrame,
        curr_p_df: pd.DataFrame,
        prev_p_df: pd.DataFrame,
        get_rev: bool = False,
    ) -> pd.DataFrame:
        if request.args.get("agg_type") == self.SHARE_AGG_TYPE:
            curr_metric_type = "share"
        else:
            curr_metric_type = "vol"

        comparison = prev_p_df.merge(curr_p_df, left_index=True, right_index=True).replace(np.inf, 0).fillna(0).astype(int)
        comparison["market_growth"] = (comparison["this_year_market_size"] - comparison["last_year_market_size"]) / comparison[
            "last_year_market_size"
        ]
        comparison["market_growth"] = (comparison["market_growth"] * 100).replace(np.inf, 0).fillna(0).astype(int)
        # comparison['host_vol_growth'] = (comparison[('this_year_volume', 'Host') if hosts else 'this_year_volume'] - comparison[ ('last_year_volume', 'Host') if hosts else 'last_year_volume']) / comparison[ ('last_year_volume', 'Host') if hosts else 'last_year_volume']
        # comparison["host_vol_growth"] = (comparison[f"host_vol_growth"] * 100).replace(np.inf, 0).fillna(0).astype(int)
        comparison = comparison.replace(np.inf, 0)

        def get_market_growth(df: pd.DataFrame, prefix: str, host_only: bool, add_rev: bool = False) -> pd.DataFrame:
            if prefix == "host":
                curr_al = "Host"
            else:
                curr_al = "Comp#1"

            df[f"{prefix}_share_this_year"] = (
                ((df["this_year_volume" if host_only else ("this_year_volume", curr_al)] / df["this_year_market_size"]) * 100)
                .replace(np.inf, 0)
                .fillna(0)
                .round(decimals=1)
            )
            df[f"{prefix}_share_last_year"] = (
                ((df["last_year_volume" if host_only else ("last_year_volume", curr_al)] / df["last_year_market_size"]) * 100)
                .replace(np.inf, 0)
                .fillna(0)
                .round(decimals=1)
            )
            df[f"{prefix}_share_growth"] = (df[f"{prefix}_share_this_year"] - df[f"{prefix}_share_last_year"]).round(decimals=2)
            df[f"{prefix}_share_gap"] = (
                (df[f"{prefix}_share_growth"] * df["this_year_market_size"] * 0.01)
                .replace(np.inf, 0)
                .fillna(0)
                .astype(int)
                .round(decimals=2)
            )

            df[f"{prefix}_vol_this_year"] = (
                (df["this_year_volume" if host_only else ("this_year_volume", curr_al)] * 1)
                .replace(np.inf, 0)
                .fillna(0)
                .round(decimals=1)
            )
            df[f"{prefix}_vol_last_year"] = (
                (df["last_year_volume" if host_only else ("last_year_volume", curr_al)] * 1)
                .replace(np.inf, 0)
                .fillna(0)
                .round(decimals=1)
            )
            df[f"{prefix}_vol_growth"] = (df[f"{prefix}_vol_this_year"] - df[f"{prefix}_vol_last_year"]) / df[
                f"{prefix}_vol_last_year"
            ]
            df[f"{prefix}_vol_growth"] = (df[f"{prefix}_vol_growth"] * 100).replace(np.inf, 0).fillna(0).astype(int)

            # if host_only :
            if add_rev and prefix == "host":
                df[f"{prefix}_rev_growth"] = (
                    (
                        (
                            (df[("this_year_rev", "Host")] / df["host_rev_this_year"])
                            - (df[("last_year_rev", "Host")] / df["host_rev_last_year"])
                        )
                        * 100
                    )
                    .replace(np.inf, 0)
                    .fillna(0)
                    .round(decimals=1)
                )
            return df

        comparison = get_market_growth(comparison, "host", len(hosts) == 0, get_rev)

        # if compatitors do exist get their growth data and merge it with comparison data
        if len(hosts) > 0:
            comparison = get_market_growth(comparison, "comp1", False)

        # cols = {('last_year_volume', 'Host'): 'host_vol_last_year',
        #         ('this_year_volume', 'Host'): 'host_vol_this_year'}

        # if len(hosts) > 0 :
        #     cols[('last_year_volume', 'Comp#1')] = 'comp1_vol_last_year'
        #     cols[('this_year_volume', 'Comp#1')] =  'comp1_vol_this_year'

        return (
            comparison.sort_values(by="this_year_market_size", ascending=False)
            .merge(
                init_df[["agency_id", "agency_name", "agency_country"]]
                .query("agency_name != 'Undisclosed Agency'")
                .drop_duplicates()
                .set_index(["agency_id", "agency_name"]),
                left_index=True,
                right_index=True,
            )
            .sort_values(by="this_year_market_size", ascending=False)
            .reset_index()
        )

    def get_comparison_as_table(self, hosts: List[str], host_map, comparison_df: pd.DataFrame):
        agg_type = request.args.get("agg_type")
        share_cols = [
            "agency_id",
            "agency_name",
            "agency_country",
            "this_year_market_size",
            "last_year_market_size",
            "market_growth",
            "host_share_this_year",
            "host_share_last_year",
            "host_share_growth",
            "host_share_gap",
        ]

        volume_cols = [
            "agency_id",
            "agency_name",
            "agency_country",
            "this_year_market_size",
            "last_year_market_size",
            "market_growth",
            "host_vol_this_year",
            "host_vol_last_year",
            "host_vol_growth",
        ]

        share_cols = (
            [
                *share_cols,
                *[
                    "comp_1",
                    "comp1_share_this_year",
                    "comp1_share_last_year",
                    "comp1_share_growth",
                    "comp1_share_gap",
                ],
            ]
            if len(hosts) > 0
            else share_cols
        )
        volume_cols = (
            [
                *volume_cols,
                *[
                    "comp_1",
                    "comp1_vol_this_year",
                    "comp1_vol_last_year",
                    "comp1_vol_growth",
                ],
            ]
            if len(hosts) > 0
            else volume_cols
        )

        cols = share_cols if agg_type == self.SHARE_AGG_TYPE else volume_cols
        table_vals = []
        for _, row in comparison_df.iterrows():
            a = {}
            for col in cols:
                if col == "comp_1":
                    a[col] = host_map["Comp#1"]
                else:
                    a[col] = row[col]
            table_vals.append(a)

        return [{e: self.AGENCY_COL_CONV[e]} for e in cols], table_vals

    @has_access("MSD", ["/agency-analysis"])
    @cache()
    @attach_story_text(STORY_TEXTS["get_agency_quadrant"])
    @attach_figure_id(["fig_agency_diag"])
    def get_agency_quadrant(self, form: AgencyQuadrant):
        periods = self.get_periods()
        pipeline = self.builder.agency_pipeline(form)
        df = self._aggregte(pipeline)

        if df.empty:
            return {
                "fig_agency_diag": self.empty_figure,
                "top_left": "Share Up/Rev Down",
                "top_right": "Share Up/Rev Up",
                "bottom_left": "Share Down/Rev Down",
                "bottom_right": "Share Down/Rev Up",
            }

        current_period_df = self.get_period_df(df, periods["current_year"], periods["current_month"])
        prev_period_df = self.get_period_df(df, periods["prev_year"], periods["prev_month"])

        mkt_share = current_period_df.groupby(["dom_op_al_code"]).agg({"pax": "sum"}) / df.pax.sum()
        comps = mkt_share.reset_index().rename(columns={"pax": "share"}).sort_values(by="share", ascending=False)

        # label carriers with Host ,Comp#<number of competitor>)
        hosts = [e for e in comps.dom_op_al_code.tolist() if e != request.user.carrier][:1]
        host_map = {e: "Comp#{}".format(i + 1) for i, e in enumerate(hosts)}
        host_map[request.user.carrier] = "Host"
        comps["al_type"] = comps["dom_op_al_code"].map(host_map)
        current_period_df = current_period_df.merge(comps, on="dom_op_al_code")
        prev_period_df = prev_period_df.merge(comps, on="dom_op_al_code")

        if (
            current_period_df.empty
            or prev_period_df.empty
            or request.user.carrier not in current_period_df.dom_op_al_code.unique().tolist()
            or request.user.carrier not in prev_period_df.dom_op_al_code.unique().tolist()
        ):
            return {
                "fig_agency_diag": self.empty_figure,
                "top_left": "Share Up/Rev Down",
                "top_right": "Share Up/Rev Up",
                "bottom_left": "Share Down/Rev Down",
                "bottom_right": "Share Down/Rev Up",
            }
        current_period_df = self.get_market_host_share(current_period_df, True, ["Host", "Comp#1"], get_rev=True)
        prev_period_df = self.get_market_host_share(prev_period_df, False, ["Host", "Comp#1"], get_rev=True)

        # df['al_type'] = df['dom_op_al_code'].apply(lambda x: 'Host' if x == request.user.carrier else 'Comp#1')
        # current_period_df = self.get_market_host_share(current_period_df,True)
        # prev_period_df = self.get_market_host_share(prev_period_df,False)

        # #get comparison_df (compare between current and prev period data)
        comparison_df = self.get_comparison_df(hosts, df, current_period_df, prev_period_df, get_rev=True)
        comparison_df["loc_flg"] = comparison_df["agency_country"].apply(lambda x: 1 if x == "Unknown Country" else 0)
        comparison_df = (
            comparison_df.sort_values(by="loc_flg", ascending=True)
            .drop_duplicates(subset=["agency_id"], keep="first")
            .reset_index(drop=True)
        )
        comparison_df = comparison_df.sort_values(by="this_year_market_size", ascending=False).iloc[:100].reset_index(drop=True)

        fig = self.figure.get_agency_quadrant_viz(
            comparison_df,
            x_col="host_rev_growth",
            y_col="host_share_growth",
        )

        response = {
            "fig_agency_diag": fig,
            "top_left": "Share Up/Rev Down",
            "top_right": "Share Up/Rev Up",
            "bottom_left": "Share Down/Rev Down",
            "bottom_right": "Share Down/Rev Up",
        }
        return response

    @has_access("MSD", ["/agency-analysis"])
    @cache()
    @attach_story_text(STORY_TEXTS["get_agency_graphs"])
    @attach_figure_id(
        [
            "fig_carrier_bd",
            "fig_ttype_bd",
            "fig_dtd_bd",
            "fig_dow_bd",
            "fig_rbd_bd",
            "fig_fare_trends",
            "fig_pax_trends",
        ]
    )
    def get_agency_graphs(self, form: AgencyGraph):
        pipeline = self.builder.agency_graph_pipeline(form)
        df = self._aggregte(pipeline)
        df.convert_currency(value_col="blended_fare", convert_to=form.get_currency())
        df.convert_currency(value_col="blended_rev", currency_col="currency", convert_to=form.get_currency())

        fig_al_bd = self.figure.get_agency_passenger_volume(df)
        fig_ttype_bd = self.figure.get_agency_ticket_type_breakdown(df)
        fig_dtd_bd = self.figure.get_agency_days_to_departure_breakdown(df)
        fig_dow_bd = self.figure.get_agency_DOW_revenue_breakdown(df)
        fig_rbd_bd = self.figure.get_agency_class_RBD_bd(df)
        fig_fare_trends = self.figure.get_agency_fare_revenue(df)
        fig_pax_trends = self.figure.get_agency_booking(df)

        response = {
            "fig_carrier_bd": fig_al_bd,
            "fig_ttype_bd": fig_ttype_bd,
            "fig_dtd_bd": fig_dtd_bd,
            "fig_dow_bd": fig_dow_bd,
            "fig_rbd_bd": fig_rbd_bd,
            "fig_fare_trends": fig_fare_trends,
            "fig_pax_trends": fig_pax_trends,
            "currency": Currency(form.get_currency()).symbol,
        }

        return response
