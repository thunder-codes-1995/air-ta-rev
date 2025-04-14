import json
import math
from datetime import date
from typing import List, Union

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from flask import request
from plotly.colors import hex_to_rgb, n_colors
from plotly.subplots import make_subplots

from base.constants import Constants
from base.dataframe import DataFrame
from base.figure import BaseFigure
from utils.funcs import add_commas, days_of_week, from_int_to_datetime


class AgencyFigure(BaseFigure):
    def get_agency_quadrant_viz(self, input_plot_df, x_col, y_col):
        month_or_year = "year"  # request.args.get("comp_type")
        """needs to be taken somewhere else"""
        """getting the colors of the bubbles"""
        colorslite = []
        colorsdark = []
        for _ in range(math.ceil((len(input_plot_df[x_col])) / 5)):
            colorslite.extend(["#EF4351", "#5D84FF", "#0AfDF7", "#B375FF", "#FFC175"])
            colorsdark.extend(["#6A0A41", "#2500AC", "#006462", "#4B0096", "#FF820D"])
            # red      blue      green     purple    yallow

        fig = go.Figure()
        size = input_plot_df["host_vol_this_year"].tolist()
        fig.add_trace(
            go.Scatter(
                x=input_plot_df[x_col].tolist(),
                y=input_plot_df[y_col].tolist(),
                # hoverinfo = 'text',
                # hovertext = input_plot_df['agency_name'].tolist(),
                textposition="top center",
                opacity=1,
                mode="markers+text",
                # fillcolor="rgb(255,0,0)",
                marker=dict(
                    opacity=0.8,
                    line=dict(color=colorsdark, width=0),
                    gradient=dict(type="radial", color=colorslite),
                    color=colorsdark,
                    size=size,
                    sizemode="area",
                    sizeref=1.0 * max(size) / (55.0**2),
                    sizemin=2,
                ),
                textfont=dict(
                    color="rgba(0, 255, 233, 0.75)",
                    family="Montserrat Semi-Bold",
                    size=13,
                ),
            )
        )

        """calculating the hover over's percenteges"""
        Var_Bkd = input_plot_df[
            "host_vol_growth"
        ]  # .apply(lambda x: round(((x[f'this_{month_or_year}_volume'] - x[f'last_{month_or_year}_volume'])/x[f'last_{month_or_year}_volume'])*100),axis=1)
        Var_Rev = input_plot_df[
            "host_rev_growth"
        ]  # .apply(lambda x: round(((x[f'this_{month_or_year}_rev'] - x[f'last_{month_or_year}_rev'])/x[f'last_{month_or_year}_rev'])*100),axis=1)

        """Adding Commas"""
        input_plot_df["host_vol_this_year"] = input_plot_df["host_vol_this_year"].apply(add_commas)
        input_plot_df["host_rev_this_year"] = input_plot_df["host_rev_this_year"].apply(add_commas)

        """hover over's data"""
        customdata = np.stack(
            (
                input_plot_df["agency_name"],
                input_plot_df["host_vol_this_year"],
                Var_Bkd,
                input_plot_df["host_rev_this_year"],
                Var_Rev,
            ),
            axis=-1,
        )
        hovertemplate = (
            "Agency Name: %{customdata[0]}<br>"
            + "<br>"
            + "Cur Ttl Bkd: %{customdata[1]}<br>"
            + "Var Bkd %: %{customdata[2]}<br>"
            + "Cur Ttl Rev: $%{customdata[3]}<br>"
            + "Var Rev %: %{customdata[4]}<br>"
            "<extra></extra>"
        )
        fig.update_traces(customdata=customdata, hovertemplate=hovertemplate)

        fig.update_layout(
            paper_bgcolor="rgb(32, 32, 68)",
            plot_bgcolor="#1E1736",
            title_font_family="Open Sans",
            title_font_size=23,
            title_x=0.5,
            height=400,
            width=1200,
            autosize=False,
            margin=dict(l=0, r=0, b=0, t=0, pad=4),
            showlegend=False,
            xaxis=dict(
                zeroline=True,
                zerolinecolor="#2E2E2E",
                autorange=True,
                # range=[-100, 100],
                # title="Share Delta",
                title_font_color="rgb(174, 181, 191)",
                title_font_size=15,
                title_font_family="Open Sans",
                gridcolor="rgb(248, 235, 247)",
            ),
            yaxis=dict(
                zeroline=True,
                zerolinecolor="#2E2E2E",
                autorange=True,
                # range=[-100, 100],
                # title="Revenue Delta",
                title_font_color="rgb(174, 181, 191)",
                title_font_size=15,
                title_font_family="Open Sans",
                gridcolor="rgb(248, 235, 247)",
            ),
        )
        for e in fig.layout:
            if "xaxis" in e or "yaxis" in e:
                fig.layout[e]["gridcolor"] = "rgb(248, 235, 247)"
                fig.layout[e]["tickfont_size"] = 13
                fig.layout[e]["tickfont_family"] = "Open Sans"
                # fig.layout[e]['zeroline']=False
                fig.layout[e]["showgrid"] = False
            for e in fig.layout["annotations"]:
                e["font"]["family"] = "Open Sans"
                e["font"]["size"] = 18

        fig.update_xaxes(color="#000000")
        fig.layout.template = None
        return json.loads(fig.to_json())

    def get_agency_ticket_type_breakdown(self, input_df: pd.DataFrame):
        fig = self.bar(
            input_df=input_df,
            x="ticket_type",
            y="pax",
            groupby=["dom_op_al_code", "ticket_type"],
            hovertemplate="Carrier : %{customdata}<br />Tkt Type: %{x}<br />Pax: %{y}<br /><extra></extra>",
            customdata_column=["dom_op_al_code"],
        )
        return json.loads(fig.to_json())

    def bar(
        self,
        input_df: pd.DataFrame,
        x: str,
        y: str,
        groupby: Union[str, List[str]],
        hovertemplate: str,
        customdata=None,
        customdata_column: str = None,
    ) -> go.Figure:
        grouped_df = input_df.groupby(groupby, as_index=False).agg({"pax": "sum"})
        colors = self.get_colors(grouped_df)
        fig = go.Figure(
            data=[
                go.Bar(
                    x=grouped_df[x],
                    y=grouped_df[y],
                    marker={"color": colors},
                    customdata=grouped_df[customdata_column] if customdata_column else customdata,
                    hovertemplate=hovertemplate,
                )
            ]
        )

        return fig

    def get_agency_days_to_departure_breakdown(self, input_df: pd.DataFrame):
        fig = self.bar(
            input_df=input_df,
            x="num_days_bins",
            y="pax",
            groupby=["dom_op_al_code", "num_days_bins"],
            hovertemplate="Carrier : %{customdata}<br />Days to departure: %{x}<br />Pax: %{y}<br /><extra></extra>",
            customdata_column=["dom_op_al_code"],
        )
        fig.layout.template = None
        return json.loads(fig.to_json())

    def get_agency_DOW_revenue_breakdown(self, input_df: pd.DataFrame):
        colors_dict = self.get_carrier_color_map(is_gradient=True, theme="light+dark")
        fig_dow_bd = make_subplots(specs=[[{"secondary_y": True}]])
        plot_df = (
            input_df.groupby(["dom_op_al_code", "travel_day_of_week"]).agg({"pax": "sum", "blended_rev": "sum"}).reset_index()
        )
        for g, g_df in plot_df.groupby("dom_op_al_code"):
            curr = g_df.sort_values(by="travel_day_of_week", ascending=True)
            travel_days_of_week = [days_of_week[i - 1] for i in curr["travel_day_of_week"]]

            fig_dow_bd.add_trace(
                go.Bar(
                    # x=curr['travel_day_of_week'].tolist(),
                    x=travel_days_of_week,
                    y=curr["pax"].tolist(),
                    legendgroup=g,
                    showlegend=False,
                    name=g,
                    marker_color=colors_dict[g][0],
                    customdata=np.stack((plot_df["dom_op_al_code"])),
                    hovertemplate="<br>Carrier: %{customdata}</br>" + "DOW: %{x}" + "<br>Booked: %{y}<br>" + " <extra></extra>",
                ),
                secondary_y=False,
            )

            fig_dow_bd.add_trace(
                go.Scatter(
                    # x=curr['travel_day_of_week'].tolist(),
                    x=travel_days_of_week,
                    y=curr["blended_rev"].tolist(),
                    legendgroup=g,
                    showlegend=False,
                    name=g,
                    marker=dict(
                        color=colors_dict[g][1],
                        size=8,
                        line=dict(color="#FFFFFF", width=3),
                    ),
                    mode="lines+markers",
                    line=dict(
                        shape="spline",
                        width=4,
                    ),
                    customdata=np.stack((plot_df["dom_op_al_code"])),
                    hovertemplate="<br>Carrier: %{customdata}</br>" + "DOW: %{x}" + "<br>Booked: %{y}<br>" + " <extra></extra>",
                ),
                secondary_y=True,
            )

        fig_dow_bd.update_layout(
            xaxis=dict(title_text=""),
            yaxis=dict(title_text=""),
            paper_bgcolor="rgb(32, 32, 68)",
            plot_bgcolor="rgb(32, 32, 68)",
            showlegend=False,
        )

        for e in fig_dow_bd.layout:
            if "xaxis" in e or "yaxis" in e:
                fig_dow_bd.layout[e]["gridcolor"] = "rgb(248, 235, 247)"
                fig_dow_bd.layout[e]["tickfont_size"] = 13
                fig_dow_bd.layout[e]["tickfont_family"] = "Open Sans"
                if "xaxis" in e:
                    fig_dow_bd.layout[e]["zeroline"] = False
                    fig_dow_bd.layout[e]["showgrid"] = False
                if "yaxis2" in e:
                    fig_dow_bd.layout[e]["showgrid"] = False

        for e in fig_dow_bd.layout["annotations"]:
            e["font"]["family"] = "Open Sans"
            e["font"]["size"] = 18

        fig_dow_bd.layout.template = None
        return json.loads(fig_dow_bd.to_json())

    def get_colors(self, data_df: pd.DataFrame, color_based_on="dom_op_al_code"):
        colors = self.get_carrier_color_map(return_list=False)
        data_df["color"] = data_df[color_based_on].apply(lambda val: colors.get(val, "#ffffff"))
        return data_df.color.tolist()

    def get_agency_passenger_volume(self, input_df: pd.DataFrame):  # Passenger Volume
        df_fig_al_bd = input_df.groupby("dom_op_al_code").agg({"pax": "sum"}).reset_index()
        colors = self.get_colors(df_fig_al_bd)
        hovertemplate = "Carrier: %{label} <br>Pax: %{value} <extra></extra>"

        fig_al_bd = go.Figure(
            go.Pie(
                values=df_fig_al_bd["pax"],
                labels=df_fig_al_bd["dom_op_al_code"],
                hovertemplate=hovertemplate,
                hole=0.5,
                marker=dict(colors=colors),
                textposition="outside",
                textfont_size=14,
                textfont_color="rgb(174, 181, 191)",
                textfont_family="Fabriga Medium",
            )
        )
        fig_al_bd.update_layout(showlegend=False)
        fig_al_bd.layout.template = None
        return json.loads(fig_al_bd.to_json())

    def get_agency_class_RBD_bd(self, input_df: pd.DataFrame):
        # FIXME waleed- Deniz i am sorry i had a problem with your code regarding the colors and other things that is why i comment it out i will work on it after the demo
        """colors_dict = self.get_carrier_color_map(is_gradient=True)

        df = input_df.groupby(['dom_op_al_code', 'seg_class', 'rbkd']).agg(
            {'blended_fare': 'mean', 'pax': 'sum'}).reset_index()

        seg_conv_mix = {'Y': 'Economy Class', 'C': 'Business Class', 'F': 'First Class', 'W': 'Premium Economy Class'}
        df['seg_class'] = df['seg_class'].map(seg_conv_mix)
        df = df.dropna()

        df_color_dict = {}
        for g, g_df in df.groupby("dom_op_al_code"):
            curr = g_df.sort_values(by='blended_fare', ascending=True).reset_index(drop=True)
            curr['color'] = n_colors(str(hex_to_rgb(colors_dict[g][0])), str(hex_to_rgb(colors_dict[g][1])), len(curr),
                                     colortype='rgb')
            for t in curr.itertuples():
                df_color_dict[(t.dom_op_al_code, t.seg_class, t.rbkd)] = t.color

        df['color'] = df.apply(lambda x: df_color_dict[x['dom_op_al_code'], x['seg_class'], x['rbkd']], axis=1)


        fig_rbd_bd = px.treemap(df, path=["dom_op_al_code", "seg_class", "rbkd"], values="pax", branchvalues="total",
                                color_discrete_sequence=df['color'],
                                hover_data=['dom_op_al_code', 'seg_class', 'rbkd', 'pax', 'blended_fare'])
        fig_rbd_bd.update_traces(root_color="rgba(0, 0, 0, 0)")
        fig_rbd_bd.update_traces(
            hovertemplate='Carrier: %{customdata[0]}<br>' + 'Class: %{customdata[1]}<br>' + 'RBD: %{customdata[2]}<br>' + 'Pax: %{customdata[3]}<br>' + 'Avg Fare: $%{customdata[4]}<br>' + ' <extra></extra>')

        fig_rbd_bd.update_layout(
            xaxis=dict(title_text=""),
            yaxis=dict(title_text=""),
            paper_bgcolor="rgb(32, 32, 68)",
            plot_bgcolor="rgb(32, 32, 68)",
            showlegend=False)


        fig_rbd_bd.layout.template = None
        fig_rbd_bd.show()"""

        colors_dict = self.get_carrier_color_map(is_gradient=True)
        agency_names = input_df["agency_name"].unique().tolist()
        agency_names = "\n".join(agency_names)
        df = (
            input_df.groupby(
                [
                    "dom_op_al_code",
                    "seg_class",
                    "rbkd",
                    "agency_name",
                ]
            )
            .agg({"blended_fare": "mean", "pax": "sum"})
            .reset_index()
        )
        seg_conv_mix = {
            "Y": "Economy Class",
            "C": "Business Class",
            "F": "First Class",
            "W": "Premium Economy Class",
        }

        df["seg_class"] = df["seg_class"].map(seg_conv_mix)
        number_of_columns = len(df["dom_op_al_code"].unique().tolist())

        graphs = []
        pax_size = []

        for g, g_df in df.groupby("dom_op_al_code"):
            for index, row in (
                g_df.groupby("dom_op_al_code").agg({"blended_fare": "mean", "pax": "sum"}).reset_index()
            ).iterrows():
                new_row = {
                    "dom_op_al_code": g,
                    "seg_class": "",
                    "rbkd": row["dom_op_al_code"],
                    "agency_name": agency_names,
                    "blended_fare": row["blended_fare"],
                    "pax": row["pax"],
                }
                g_df = g_df.append(new_row, ignore_index=True)

            temp_df = g_df.groupby("seg_class").agg({"blended_fare": "mean", "pax": "sum"}).reset_index().iterrows()
            for index, row in temp_df:
                if row["seg_class"] != "":
                    # g_df.loc[len(g_df)] = [g, g, row["seg_class"], agency_names, row["blended_fare"], row["pax"]]
                    new_row = {
                        "dom_op_al_code": g,
                        "seg_class": g,
                        "rbkd": row["seg_class"],
                        "agency_name": agency_names,
                        "blended_fare": row["blended_fare"],
                        "pax": row["pax"],
                    }
                    g_df = g_df.append(new_row, ignore_index=True)

            g_df["blended_fare"] = g_df["blended_fare"].apply(lambda x: round(x))
            g_df = g_df.sort_values("blended_fare")

            g_df["color"] = n_colors(
                str(hex_to_rgb(colors_dict[g][0])),
                str(hex_to_rgb(colors_dict[g][1])),
                len(g_df),
                colortype="rgb",
            )
            treemap = go.Treemap(
                labels=g_df["rbkd"],
                parents=g_df["seg_class"],
                values=g_df["pax"],
                insidetextfont=dict(color="white", size=12, family="Fabriga"),
                root=dict(color="rgba(0, 0, 0, 0)"),
                branchvalues="total",
                marker_colors=g_df["color"],
                customdata=np.stack(
                    (g_df["dom_op_al_code"], g_df["agency_name"], g_df["blended_fare"]),
                    axis=-1,
                ),
                hovertemplate="Agency: %{customdata[1]}<br>"
                + "Carrier: %{customdata[0]}<br>"
                + "Class: %{label}<br>"
                + "Pax: %{value}<br>"
                + "Avg Fare: $%{customdata[2]}<br>"
                + " <extra></extra>",
            )
            graphs.append(treemap)
            pax_size.append(sum(g_df["pax"]))

        fig_rbd_bd = make_subplots(
            rows=1,
            cols=number_of_columns,
            specs=[[{"type": "treemap"}] * number_of_columns],
            horizontal_spacing=0,
            column_widths=pax_size,
        )
        for idx, graph in enumerate(graphs):
            fig_rbd_bd.add_trace(graph, row=1, col=idx + 1)

        fig_rbd_bd.update_layout(
            xaxis=dict(title_text=""),
            yaxis=dict(title_text=""),
            paper_bgcolor="rgb(32, 32, 68)",
            plot_bgcolor="rgb(32, 32, 68)",
            showlegend=False,
        )

        fig_rbd_bd.update_coloraxes(
            colorbar=dict(
                title="Blended Fare ($)",
                title_font_size=13,
                title_font_color="rgb(174, 181, 191)",
                title_font_family="Open Sans",
                tickfont=dict(family="Open Sans", size=12, color="rgb(174, 181, 191)"),
            )
        )
        fig_rbd_bd.data[0]["textfont"]["color"] = "white"
        fig_rbd_bd.layout.template = None
        return json.loads(fig_rbd_bd.to_json())

    def scatter(
        self,
        input_df: pd.DataFrame,
        x: str,
        y: str,
        custom_column: str = None,
        customdata: str = None,
        mode: str = "lines",
        hovertemplate: str = None,
        line={},
    ) -> go.Scatter:
        return go.Scatter(
            x=input_df[x],
            y=input_df[y],
            showlegend=False,
            mode=mode,
            line={**dict(shape="spline", width=6), **line},
            customdata=input_df[custom_column] if custom_column else customdata,
            hovertemplate=hovertemplate,
        )

    def aggregate(self, input_df: DataFrame):
        groupby = ["dom_op_al_code", "date"]

        # add date column
        if request.args.get("comp_type") in Constants.AGG_VIEW_YEARLY:
            groupby.extend(["travel_year", "travel_month"])
            input_df.add_date(year_col="travel_year", month_col="travel_month")
        else:
            input_df.date_as_str("travel_date", "temp")
            input_df.date_from_int("temp")

        # caculate fare and sum of passenger
        df = input_df.groupby(groupby, as_index=False).agg({"pax": "sum", "blended_fare": "mean"})
        df.sort_values(by="date", inplace=True)
        return df

    def handle_scatter_figures(self, input_df: pd.DataFrame, y_column: str):
        colors_map = self.get_carrier_color_map()
        fare_pax_trends_df = self.aggregate(input_df)
        fig = go.Figure()

        for carrier, sub_df in fare_pax_trends_df.groupby("dom_op_al_code"):
            fig.add_trace(
                self.scatter(
                    input_df=sub_df,
                    x="date",
                    y=y_column,
                    custom_column="dom_op_al_code",
                    hovertemplate="Carrier: %{customdata}<br />Date: %{x}<br />Booked: %{y}<br /><extra></extra>",
                    line={"color": colors_map.get(carrier, "#ffffff")},
                ),
            )

        if request.args.get("comp_type") in Constants.AGG_VIEW_YEARLY:
            fig.update_xaxes(dtick="M3", tickformat="%b-%Y")
        else:
            fig.update_xaxes(tickformat="%d-%m-%Y")

        fig.layout.template = None
        return json.loads(fig.to_json())

    def get_agency_booking(self, input_df: pd.DataFrame):
        return self.handle_scatter_figures(input_df, "pax")

    def get_agency_fare_revenue(self, input_df: pd.DataFrame):
        return self.handle_scatter_figures(input_df, "blended_fare")
