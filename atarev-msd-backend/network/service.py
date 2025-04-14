from typing import Dict, List, Union

import pandas as pd
from flask import request

from airports.repository import AirportRepository
from base.constants import Constants
from base.helpers.permissions import has_access
from base.helpers.routes import ProtectedRoutes
from base.middlewares import attach_carriers_colors, attach_figure_id, attach_story_text, cache
from base.service import BaseService
from dds.repository import DdsRepository
from network.builder import NetworkBuilder
from network.figure import NetworkFigure
from network.forms import NetworkByondPoints, NetworkConictivityMap, NetworkSchedulingComparisonDetails
from network.handler import NetworkBeyondPointsHandler, NetworkHandler
from network.labels import get_schedule_tables

airport_repository = AirportRepository()

builder = NetworkBuilder()
dds_repo = DdsRepository()
figure = NetworkFigure()


class NetworkService(BaseService):

    handler_class = NetworkHandler

    INBOUND = "inbound"
    OUTBOUND = "outbound"

    MAP_LABELS = [
        {"dom_op_al_code": "CARR"},
        {"bound": "INB/OUT"},
        {"path": "O&D"},
        {"pax_sum": "PAX"},
        {"blended_rev": "REV ($)"},
    ]

    STORY_TEXTS = {
        "network_beyond_points": {"main_card": {"title": "test", "content": "test"}},
        "network_comparison_details": {"main_card": {"title": "test", "content": "test"}},
        "network_conictivity_map": {"main_card": {"title": "test", "content": "test"}},
    }

    def get_handler_class(self):
        handler_class = (
            NetworkBeyondPointsHandler
            if ProtectedRoutes.GET_NETWORK_SCHEDULING_BEYOND_POINTS.value in request.path
            else NetworkHandler
        )
        return handler_class

    @has_access("MSD", ["/network-scheduling"])
    @cache()
    @attach_story_text(STORY_TEXTS["network_beyond_points"])
    @attach_figure_id(["fig_inbound", "fig_outbound"])
    def network_beyond_points(self, form: NetworkByondPoints):
        summery_pipeline = builder.beyond_points_summery_pipeline(form)
        summery_inbound_pipeline = builder.beyond_points_inbound_summery_pipeline(form)
        summery_outbound_pipeline = builder.beyond_points_outbound_summery_pipeline(form)

        # get results for previouse 3 queries
        summary_df = pd.DataFrame(dds_repo.aggregate(summery_pipeline))
        summary_inbound_df = pd.DataFrame(dds_repo.aggregate(summery_inbound_pipeline))
        summary_outbound_df = pd.DataFrame(dds_repo.aggregate(summery_outbound_pipeline))

        fig_in, fig_out = figure.indirect_bdown_viz(summary_inbound_df, summary_outbound_df)
        summary_df = pd.DataFrame(self.handler.handle_rows(summary_df))

        response = {
            "fig_inbound": fig_in or self.empty_figure,
            "fig_outbound": fig_out or self.empty_figure,
            "metrics_cart": self.__get_metrics_cart(summary_df),
            "legend_scale": self.__get_beyond_points_legend_scale(summary_inbound_df, summary_outbound_df),
        }

        return response

    def __get_beyond_points_legend_scale(self, inbound_df: pd.DataFrame, outbound_df: pd.DataFrame):
        inbound = inbound_df.blended_fare.unique().tolist() if not inbound_df.empty else []
        outbound = outbound_df.blended_fare.unique().tolist() if not outbound_df.empty else []
        return sorted([*inbound, *outbound])

    def __get_metrics_cart(self, summary_df: pd.DataFrame):
        col_to_title = {
            "pax": "Total Passengers",
            "blended_rev": "Total Revenue",
            "blended_fare": "Average Fare",
        }
        types = summary_df.is_direct.unique().tolist()
        return [
            {
                "cart_title": col_to_title[col],
                "metrics": [
                    {
                        "title": "Beyond",
                        "value": (
                            summary_df.query("is_direct == 'Indirect'").iloc[0][col]
                            if "Indirect" in types
                            else 0 if not summary_df.empty else None
                        ),
                    },
                    {
                        "title": "Point-to-Point",
                        "value": (
                            summary_df.query("is_direct == 'Direct'").iloc[0][col]
                            if "Direct" in types
                            else 0 if not summary_df.empty else None
                        ),
                    },
                ],
            }
            for col in ["pax", "blended_rev", "blended_fare"]
        ]

    @has_access("MSD", ["/network-scheduling"])
    @cache()
    @attach_story_text(STORY_TEXTS["network_comparison_details"])
    @attach_figure_id(["fig"])
    def network_comparison_details(self, form: NetworkSchedulingComparisonDetails):
        pipeline = self.builder.network_comparison_details_pipeline(form)
        df = self._aggregte(pipeline)
        if df.empty:
            return {"table": {"data": [], "labels": []}, "fig": self.empty_figure}
        df = self.__add_local_dep_hour(df)
        df = self.__get_text_week_day(df)
        df = self.__get_carrier_color(df)
        df.convert_currency(value_col="blended_rev", convert_to=form.get_currency())
        fig = figure.scheduling_viz(df)

        fig_ = {
            "table": {
                "data": self.__build_schedule_table(df),
                "labels": get_schedule_tables(df),
            },
            "fig": fig,
        }
        return fig_

    def __get_text_week_day(self, df: pd.DataFrame) -> pd.DataFrame:
        df["week_day_str"] = df.travel_day_of_week.map(Constants.IDX2WEEKDAY)
        return df

    def __add_local_dep_hour(self, df: pd.DataFrame) -> pd.DataFrame:
        def local_dep_time_to_hour(val: int) -> str:
            """
            convert numeric time to string but take only full hour
            example: 1223 --> '12:00'
            """
            val = f"{val}"
            if not val.isnumeric():
                return "--:--"
            if len(f"{val}") == 4:
                return f"{val[0:2]}:00"
            if len(f"{val}") == 3:
                return f"0{val[0:1]}:00"
            return f"00:00"

        df["local_dep_hour"] = df.local_dep_time.apply(lambda x: local_dep_time_to_hour(x))

        return df

    def __get_carrier_color(self, df: pd.DataFrame) -> pd.DataFrame:
        carrier_color_map = self.get_carrier_color_map()
        df["color"] = df.dom_op_al_code.map(carrier_color_map)
        return df

    def __build_schedule_table(self, df: pd.DataFrame):
        g_df = df.groupby(
            ["dom_op_al_code", "local_dep_hour", "equip", "travel_day_of_week"],
            as_index=False,
        ).agg(
            {
                "pax": "sum",
                "blended_rev": "sum",
                "next_dest": lambda dests: ", ".join(set(dests)),
            }
        )
        g_df = g_df.sort_values(
            by=["dom_op_al_code", "travel_day_of_week", "local_dep_hour"],
            ascending=True,
        )
        g_df = self.__get_text_week_day(g_df)
        g_df["currency"] = g_df.shape[0] * [df.currency.iloc[0]]
        return self.handler.handle_rows(g_df)

    def get_countries_for_airports(self, codes: List[str]) -> List[str]:
        """takse list of airport codes  and returns list of corresponding country codes"""
        return {item["country_code"]: item["airports"] for item in airport_repository.get_airports_grouped_by_country(codes)}
        # mp = airport_repository.get_country_airport_map()
        # self.AIRPORT_COUNTRY_MAP = mp
        # return [self.AIRPORT_COUNTRY_MAP[code] for code in codes]

    @has_access("MSD", ["/network-scheduling"])
    @cache()
    @attach_carriers_colors()
    @attach_story_text(STORY_TEXTS["network_conictivity_map"])
    @attach_figure_id(["fig"])
    def network_conictivity_map(self, form: NetworkConictivityMap):
        pipeline = self.builder.network_conictivity_map_pipeline(form)
        df = self._aggregte(pipeline)

        if df.empty:
            return {
                "fig": {},
                "legend_scale": [],
                "table": {"labels": [], "data": []},
                "mini_kpis": [],
                "airline_options": [],
            }

        df = self.__get_targeted_conectivity_flights(df, form)
        df = self.__get_connectivity_coords_df(df)
        fig, legend_scale = figure.destinations_map_viz(df, form)

        return {
            "fig": fig,
            "legend_scale": legend_scale,
            "table": {"labels": self.MAP_LABELS, "data": self.handler.handle_rows(df)},
            "mini_kpis": self.__get_connectivity_kpis(df, form),
            "airline_options": self.__get_connectivity_airline_options(form),
        }

    def __get_connectivity_airline_options(self, form):
        carriers = [request.user.carrier]
        main = form.get_main_competitor()
        others = form.get_selected_competitors_list()
        if main:
            carriers.append(main)
        return list(set(carriers + others))

    def __get_connectivity_kpis(self, data_df: pd.DataFrame, form: NetworkConictivityMap):
        counts = self.__get_kpi_counts(data_df, form)
        return [
            {"title": "Host IB", "value": counts[0]},
            {"title": "Host OB", "value": counts[1]},
            {"title": "Comp1 IB", "value": counts[2]},
            {"title": "Comp1 OB", "value": counts[3]},
            {"title": "Comp2 IB", "value": counts[4]},
            {"title": "Comp2 OB", "value": counts[5]},
        ]

    def __get_kpi_counts(self, data_df: pd.DataFrame, form: NetworkConictivityMap) -> List[Union[int, str]]:
        # result should always contain counts for comp1 and comp2 along with host carrier
        # so i need to add comp1,comp2 (in case user didn't select competitors)
        # then i slice (take the first 3) thus i guarantee i have all competitors along with host
        comps = [
            *[
                comp
                for comp in [
                    request.user.carrier,
                    form.get_main_competitor(),
                    *form.get_selected_competitors_list(),
                ]
                if comp
            ],
            "comp1",
            "comp2",
        ][0:3]
        grouped = data_df.groupby(["dom_op_al_code", "bound"], as_index=False).agg({"path": "count"})

        # this _map is gonna be like this : {PY-Inbound : 5,PY-Outbound : 10}
        _map = {"-".join(i[0]): i[1] for i in (zip(zip(grouped.dom_op_al_code, grouped.bound), grouped.path))}

        # for each carrier in (host, competitors) list return counts for both Inbound and Outbound
        return [_map.get(f"{comp}-{bound}", "-") for comp in comps for bound in ["Inbound", "Outbound"]]

    def __get_connectivity_coords_df(self, targeted_conectivity_df: pd.DataFrame) -> pd.DataFrame:
        """add coordinates data for airports"""
        targeted_conectivity_df[["start_city", "end_city"]] = targeted_conectivity_df.path.str.split("-", expand=True)
        all_airports = list(
            {
                *targeted_conectivity_df.start_city.unique().tolist(),
                *targeted_conectivity_df.end_city.unique().tolist(),
            }
        )
        airports_coordinates_map = airport_repository.get_airports_coordinates_map(all_airports)
        data_as_list = targeted_conectivity_df.to_dict("records")
        data = []

        for flight in data_as_list:
            start = airports_coordinates_map.get(flight["start_city"])
            end = airports_coordinates_map.get(flight["end_city"])

            if start and end:
                data.append(
                    {
                        "start_lat": start[0],
                        "start_long": start[1],
                        "end_lat": end[0],
                        "end_long": end[1],
                        **flight,
                    }
                )

        return pd.DataFrame(data)

    def __get_targeted_conectivity_flights(self, data_df: pd.DataFrame, form: NetworkConictivityMap) -> pd.DataFrame:
        """
        if a flight is inbound and country(prev_dest) is in destination_counties -> skip
        if a flight is outbound and country(next_dest) is in origin_countries -> skip
        """
        # get airports' countries for both selected origins and destinations
        origin_countries = self.get_countries_for_airports(form.get_orig_city_airports_list())
        dest_countries = self.get_countries_for_airports(form.get_dest_city_airports_list())

        def does_airport_belong_to_country(airport: str, mp: Dict[str, List[str]]) -> bool:
            belongs = False
            countries = list(mp.keys())

            i = 0
            while not belongs and i < len(countries):
                if airport in mp[countries[i]]:
                    belongs = True
                i += 1

            return belongs

        data_df["flag"] = data_df.apply(
            lambda df:
            # if a flight is inbound and country(prev_dest) is in destination_countries -> skip
            (
                0
                if does_airport_belong_to_country(df["path"].split("-")[1], dest_countries) and df.bound == "Inbound"
                # if a flight is outbound and country(next_dest) is in origin_countries -> skip
                else (
                    0
                    if does_airport_belong_to_country(df["path"].split("-")[1], origin_countries) and df.bound == "Outbound"
                    else 1
                )
            ),
            axis=1,
        )

        return data_df[data_df.flag == 1]
