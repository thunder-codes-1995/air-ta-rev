import json
from typing import List

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from flask import request

from base.entities.market import Bridge
from base.figure import BaseFigure
from base.handler import FieldsHandler
from network.forms import NetworkConictivityMap


class NetworkFigure(BaseFigure):
    handler = FieldsHandler()

    def indirect_bdown_viz(self, plot_in, plot_out):
        fig_in, fig_out = None, None
        colors = self.__get_beyond_points_colors(plot_in, plot_out)

        if not plot_in.empty:
            fig_in = self.__plot_beyond_points(plot_in, colors, True)
            fig_in.update_traces(
                hovertemplate="<b>O-D: </b>%{customdata[3]}-%{customdata[2]}<br>"
                + "<b>Pax: </b>%{customdata[0]}<br>"
                + "<b>Avg Fare: </b>$%{customdata[1]:.0f}"
            )
            fig_in.update_coloraxes(
                colorbar=dict(
                    title="Average Fare",
                    title_font_size=13,
                    title_font_color="rgb(174, 181, 191)",
                    title_font_family="Open Sans",
                    x=1.0,
                    tickprefix=" $",
                    len=0.8,
                    tickfont=dict(
                        family="Open Sans",
                        size=12,
                    ),
                ),
                showscale=False,
            )
            fig_in = self.__update_beyond_points_layout(fig_in)
            fig_in = json.loads(fig_in.to_json())

        if not plot_out.empty:
            fig_out = self.__plot_beyond_points(plot_out, colors, False)
            fig_out.update_traces(
                hovertemplate="<b>O-D: </b>%{customdata[2]}-%{customdata[3]}<br><b>Pax: </b>%{customdata[0]}<br><b>Avg Fare: </b>$%{customdata[1]:.0f}"
            )
            fig_out.update_coloraxes(showscale=False)
            fig_out = self.__update_beyond_points_layout(fig_out)
            fig_out = json.loads(fig_out.to_json())

        return fig_out, fig_in

    def __get_beyond_points_colors(self, plot_in_df: pd.DataFrame, plot_out_df: pd.DataFrame):
        colors = []
        if not plot_in_df.empty:
            colors += plot_in_df.blended_fare.unique().tolist()
        if not plot_out_df.empty:
            colors += plot_out_df.blended_fare.unique().tolist()

        colors_range = self.n_colors(["rgb(228, 31, 120)", "rgb(0, 255, 233)"], len(colors)) if colors else []
        return colors_range

    def __plot_beyond_points(self, df: pd.DataFrame, colors: List[str], is_in_plot: bool):
        params = {
            "path": ["dom_op_al_code", "orig_code", "prev_dest"] if is_in_plot else ["dom_op_al_code", "dest_code", "next_dest"],
            "values": "pax",
            "color": "blended_fare",
            "hover_data": (
                ["pax", "blended_fare", "orig_code", "prev_dest"]
                if is_in_plot
                else ["pax", "blended_fare", "dest_code", "next_dest"]
            ),
        }
        if colors:
            params["color_continuous_scale"] = colors
        return px.sunburst(df, **params)

    def __update_beyond_points_layout(self, fig):
        fig.update_layout(
            width=350, height=250, autosize=False, margin=dict(l=0, r=0, b=0, t=5, pad=2), paper_bgcolor="rgb(32, 32, 68)"
        )
        fig.layout.template = None
        return fig

    def scheduling_viz(self, curr_sched):
        fig = go.Figure()
        fig.add_trace(
            go.Parcats(
                dimensions=[
                    {
                        "label": "Day-of-Week",
                        "values": curr_sched["week_day_str"],
                        "categoryarray": curr_sched.drop_duplicates(["travel_day_of_week", "week_day_str"]).sort_values(
                            by="travel_day_of_week", ascending=True
                        )["week_day_str"],
                        "categoryorder": "array",
                    },
                    {"label": "Carrier", "values": curr_sched["dom_op_al_code"]},
                    {
                        "label": "Departure Hour",
                        "values": curr_sched.local_dep_hour,
                        "categoryarray": curr_sched.drop_duplicates(["local_dep_hour"]).sort_values(
                            by="local_dep_hour", ascending=True
                        )["local_dep_hour"],
                        "categoryorder": "array",
                    },
                ],
                # counts=curr_sched.freq.tolist(),
                line=dict(color=curr_sched.color.tolist()),
                tickfont=dict(family=self.style.font_family, size=15),
                labelfont=dict(color=self.style.font_color, family=self.style.font_family, size=15),
            )
        )

        fig = self.style_figure(
            fig,
            layout={
                "title_font_size": 23,
                "width": 750,
                "height": 250,
                "showlegend": True,
                "margin": {
                    "l": 10,
                    "r": 10,
                    "b": 0,
                    "t": 40,
                    "pad": 4,
                },
            },
        )
        return json.loads(fig.to_json())

    def destinations_map_viz(self, map_df: pd.DataFrame, form: NetworkConictivityMap):
        airports_df = self.__get_airports_coordinates_df(map_df)
        airports_df = self.__connection_points_hover_text(airports_df)
        fig = self.__add_knot_points_to_map(map_df, form)
        fig, legend_scale = self.__add_connection_points_to_map(airports_df, form, fig)
        airports_df = airports_df.merge(
            map_df, left_on=["dom_op_al_code", "bound", "airport"], right_on=["dom_op_al_code", "bound", "end_city"]
        )
        fig = self.__add_conenctions_to_map(airports_df, fig, form)
        fig = self.style_figure(
            fig,
            legend_title="Carriers",
            layout={
                "title_x": 0.5,
                "width": 1230,
                "height": 550,
                "legend_title_font_size": self.style.font_size("sm"),
                "legend_itemsizing": "trace",
                "legend_itemclick": "toggle",
                "legend_itemdoubleclick": "toggleothers",
                "legend_xanchor": "left",
                "title_font_size": self.style.title_font_size,
                "legend_traceorder": "normal",
                "legend_yanchor": "auto",
                "legend_valign": "middle",
                "dragmode": "zoom",
                "geo": {
                    "showland": True,
                    "showcountries": True,
                    "showocean": True,
                    "showlakes": False,
                    "showrivers": False,
                    "oceancolor": "rgb(32, 32, 68)",
                    "landcolor": "rgb(32, 32, 68)",
                    "resolution": 110,
                    "showframe": False,
                    "framewidth": 0,
                    "countrywidth": 1,
                    "countrycolor": "rgba(0, 255, 233, 0.75)",
                    "coastlinewidth": 1,
                    "showcoastlines": False,
                    "fitbounds": "locations",
                    "bgcolor": "rgb(32, 32, 68)",
                },
            },
        )
        return json.loads(fig.to_json()), legend_scale

    def __get_airports_coordinates_df(self, map_df: pd.DataFrame):
        return map_df[["end_city", "end_lat", "end_long", "blended_rev", "pax_sum", "bound", "dom_op_al_code"]].rename(
            columns={"end_city": "airport", "end_lat": "lat", "end_long": "long"}
        )

    def __add_knot_points_to_map(self, airports_df: pd.DataFrame, form: NetworkConictivityMap):
        """add knots points (selected origin | destination airports) to map"""
        selected_markets = form.get_orig_city_airports_list() + form.get_dest_city_airports_list()
        knots_df = (airports_df[airports_df.start_city.isin(selected_markets)]).drop_duplicates("start_city")

        fig = go.Figure()
        fig.add_trace(
            go.Scattergeo(
                lon=knots_df["start_long"],
                lat=knots_df["start_lat"],
                hoverinfo="text",
                text=knots_df["start_city"],
                mode="markers",
                textfont_size=16,
                textposition="top right",
                showlegend=False,
                marker=dict(
                    symbol="triangle-up",
                    size=15,
                ),
            )
        )

        return fig

    def __add_connection_points_to_map(self, airports_df: pd.DataFrame, form: NetworkConictivityMap, fig):
        """add connection points to map"""
        selected_markets = form.get_orig_city_airports_list() + form.get_dest_city_airports_list()
        connections_df = (
            (airports_df[~airports_df.airport.isin(selected_markets)])
            .groupby(["airport", "long", "lat"], as_index=False)
            .agg({"blended_rev": "sum"})
        )

        if connections_df.empty:
            return fig
        blended_revs = connections_df[["blended_rev"]]
        colors = self.n_colors(
            ["rgb(228, 31, 120)", "rgb(0, 255, 233)"],
            blended_revs.drop_duplicates().sort_values(by="blended_rev", ascending=True).shape[0],
        )

        connections_df = connections_df.merge(airports_df[["airport", "hover_text"]], on=["airport"])

        fig.add_trace(
            go.Scattergeo(
                lon=connections_df["long"],
                lat=connections_df["lat"],
                hoverinfo="text",
                hovertext=connections_df["hover_text"],
                textfont_size=10,
                textfont_color="white",
                textfont_family=self.style.font_family,
                mode="markers",
                showlegend=False,
                marker=dict(
                    showscale=False,
                    size=8,
                    opacity=0.5,
                    color=connections_df["blended_rev"],
                    colorscale=colors,
                    cmin=connections_df["blended_rev"].min(),
                    cmax=connections_df["blended_rev"].max(),
                    colorbar=dict(
                        title="Total Revenue",
                        title_font_size=13,
                        title_font_color=self.style.font_color,
                        title_font_family=self.style.font_family,
                        x=1.2,
                        y=0.6,
                        tickprefix=" $",
                        len=0.8,
                        tickfont=dict(family=self.style.font_family, size=12, color=self.style.font_color),
                    ),
                ),
            )
        )
        return fig, self.__get_conenctivity_legend_scale(connections_df)

    def __get_conenctivity_legend_scale(self, df: pd.DataFrame):
        blended_rev_df = df[["blended_rev"]].drop_duplicates().sort_values(by="blended_rev")
        return [val for val in blended_rev_df.blended_rev.tolist()]

    def __add_conenctions_to_map(self, map_df: pd.DataFrame, fig, form: NetworkConictivityMap):
        market = Bridge(
            request.user.carrier,
            form.get_orig_city_airports_list(),
            form.get_dest_city_airports_list(),
        ).get_city_based_makret()
        
        carriers_colors_map = market.carrier_color_map(form.get_theme())
        bounds = [bound.capitalize() for bound in form.bound_selection.data.split(",")]
        map_df = map_df[map_df.bound.isin(bounds)]

        for _, row in map_df.iterrows():
            line_mode = "dash" if row["bound"] == "Outbound" else None
            leg = f"{row['dom_op_al_code']}#{row['bound']}"
            fig.add_trace(
                go.Scattergeo(
                    lon=[row["start_long"], row["end_long"]],
                    lat=[row["start_lat"], row["end_lat"]],
                    mode="lines",
                    line=dict(dash=line_mode, width=1, color=carriers_colors_map[row["dom_op_al_code"]]),
                    name=row["dom_op_al_code"] + " - " + row["bound"].capitalize(),
                    showlegend=False,
                    legendgroup=leg,
                    opacity=1.0,
                    visible=True,
                )
            )

        return fig

    def __connection_points_hover_text(self, airports_df: pd.DataFrame):
        airports_df_grouped = (
            airports_df.groupby(["airport", "dom_op_al_code", "bound"], as_index=False)
            .agg({"pax_sum": "sum", "blended_rev": "sum"})
            .query("pax_sum >= 10")
        )

        airports_df_grouped["blended_fare"] = (airports_df_grouped["blended_rev"] / airports_df_grouped["pax_sum"]).astype(int)

        text_dict = {}

        for g, g_df in airports_df_grouped.groupby("airport"):
            curr_str = [f"<b>{g}</b>"]
            for _, g_i_df in g_df.groupby("dom_op_al_code"):
                inner_str = []
                for r in g_i_df.itertuples():
                    inner_str.append(f"{r.dom_op_al_code}-{r.bound}: {r.pax_sum:,}; ${r.blended_rev:,}; ${r.blended_fare}")
                curr_str.append("<br>".join(inner_str))
            curr_str = "<br>".join(curr_str)

            text_dict[g] = curr_str

        merged = airports_df[["dom_op_al_code", "bound", "airport", "lat", "long"]].merge(
            airports_df_grouped, on=["dom_op_al_code", "bound", "airport"]
        )
        merged["hover_text"] = merged["airport"].map(text_dict)
        return merged
