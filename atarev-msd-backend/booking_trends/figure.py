import json
from datetime import date

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from flask import request

from base.constants import Constants
from base.entities.market import Bridge
from base.figure import BaseFigure
from booking_trends.forms import BookingTrends
from filters.repository import FilterRepository


class BookingTrendsFigure(BaseFigure):
    OVERALL_BOOKING_CURVE_AGG = "overall"
    DOW_BOOKING_CURVE_AGG = "day-of-week"
    WEEK_TIME_BOOKING_CURVE_AGG = "day-of-week-time"
    filter_repository = FilterRepository()

    # def booking_trend_viz(self, df: pd.DataFrame, holiday_df: pd.DataFrame, form: BookingTrends):
    #     fig = go.Figure()

    #     # add chart columns
    #     df = self.add_label_text(df)
    #     df, holiday_df = self.add_x_axis(df, holiday_df)

    #     market = Bridge(
    #         request.user.carrier,
    #         form.get_orig_city_airports_list(),
    #         form.get_dest_city_airports_list(),
    #     ).get_city_based_makret()

    #     get_carrier_color_map = market.carrier_color_map(form.get_theme())
    #     for al, color in get_carrier_color_map.items():
    #         curr = df.query("{} == '{}'".format("dom_op_al_code", al)).sort_values(by="travel_date", ascending=True)
    #         if curr.empty:
    #             continue
    #         fig.add_trace(
    #             go.Scatter(
    #                 x=curr["travel_date"].tolist(),
    #                 y=curr["pax"].tolist(),
    #                 text=curr["det_text"].tolist(),
    #                 showlegend=True,
    #                 line=dict(color=color, dash="solid", smoothing=1.3, shape="spline", width=6),
    #                 name=al,
    #                 hoverinfo="text",
    #                 opacity=1.0,
    #                 mode="lines",
    #                 legendgroup=al,
    #             )
    #         )

    #     fig = self.add_holidays_to_plot(fig, df, holiday_df)
    #     fig = self.set_layout(fig)
    #     fig = self.update_layouts(fig)
    #     fig.layout.template = None
    #     return json.loads(fig.to_json())

    # def add_holidays_to_plot(self, fig, df, holiday_df):
    #     """add holiday data (from host perspective) to plot"""

    #     holiday_df = self.get_holiday_data(df, holiday_df)
    #     holiday_df["holiday_end_date"] = pd.to_datetime(holiday_df["holiday_end_date"])
    #     if holiday_df.empty:
    #         return fig

    #     def format_holiday_text(row):
    #         result = row["holiday_country"] + " - " + row["holiday_name"] + " - "
    #         start_date = row["holiday_start_date"].strftime("%d%b%y").upper()
    #         end_date = row["holiday_end_date"].strftime("%d%b%y").upper()
    #         if start_date == end_date:
    #             result += start_date
    #         else:
    #             start_day = start_date[:2]
    #             result += start_day + "-" + end_date
    #         return result

    #     holiday_df = holiday_df.groupby(["holiday_start_date", "holiday_country", "holiday_name"]).first().reset_index()
    #     holiday_df_g = holiday_df.groupby("holiday_start_date")

    #     for start_date, group in holiday_df_g:
    #         group["hover_text"] = group.apply(format_holiday_text, axis=1)
    #         text = group["hover_text"].str.cat(sep="<br>")
    #         fig.add_trace(
    #             go.Scatter(
    #                 line=dict(color="#0dcaf0"),
    #                 x=[start_date],
    #                 y=[group["pax"].iloc[0]],
    #                 text=text,
    #                 showlegend=False,
    #                 hoverinfo="text",
    #                 mode="markers",
    #                 hoverlabel=dict(align="left", bgcolor="#9c00ed"),
    #             )
    #         )

    #     return fig

    # def __get_holiday_monthly(self, df: pd.DataFrame, holiday_df: pd.DataFrame):
    #     """get holiday_data based on host perspective for monthly agg_type"""
    #     if holiday_df.empty:
    #         return holiday_df
    #     host_df = df[df["dom_op_al_code"] == f"{request.user.carrier}"][["pax", "travel_date"]]
    #     merged = host_df.merge(holiday_df, right_on="holiday_start_date", left_on="travel_date")
    #     return merged

    # def __get_holiday_daily(self, df: pd.DataFrame, holiday_df: pd.DataFrame):
    #     """get holiday_data based on host perspective for daily agg_type"""
    #     curr_holiday_plot_df = (
    #         holiday_df.sort_values(by="holiday_start_date", ascending=True)
    #         .groupby(["holiday_year", "holiday_month"])
    #         .agg({"date_name": lambda x: "<br>".join(list(x))})
    #         .reset_index()
    #         .rename(columns={"date_name": "holiday_name"})
    #     )

    #     if curr_holiday_plot_df.empty:
    #         return curr_holiday_plot_df

    #     curr_holiday_plot_df["holiday_start_date"] = curr_holiday_plot_df.apply(
    #         lambda x: date(x["holiday_year"], x["holiday_month"], 1), axis=1
    #     )
    #     host_df = df[df["dom_op_al_code"] == f"{request.user.carrier}"][["pax", "travel_date"]]
    #     merged = host_df.merge(holiday_df, right_on="holiday_start_date", left_on="travel_date")
    #     return merged

    # def get_holiday_data(self, df: pd.DataFrame, holiday_df: pd.DataFrame):
    #     """get holiday data from host perspective based on agg_type"""
    #     if request.args.get("agg_type") != Constants.AGG_VIEW_MONTHLY:
    #         return self.__get_holiday_daily(df, holiday_df)
    #     return self.__get_holiday_monthly(df, holiday_df)

    # def add_x_axis(self, df, holiday_df):
    #     """
    #     add travel date column which will serve as x-axis
    #     both travel_date and holiday_date_start should be
    #     date objects like : (YYYY-MM-01) when agg_type == monthly else (YYYY-MM-DD)
    #     """
    #     if request.args.get("agg_type") == Constants.AGG_VIEW_MONTHLY:
    #         df["travel_date"] = df.apply(lambda _df: date(_df.travel_date.year, _df.travel_date.month, 1), axis=1)
    #         if not holiday_df.empty:
    #             holiday_df["holiday_start_date"] = holiday_df.apply(
    #                 lambda _df: date(_df.holiday_year, _df.holiday_month, 1), axis=1
    #             )
    #     return df, holiday_df

    # def add_label_text(self, df):
    #     """add label text column based on agg_type"""
    #     df["pax"] = df["pax"].astype(int)
    #     if request.args.get("agg_type") == Constants.AGG_VIEW_MONTHLY:
    #         df["det_text"] = df.apply(
    #             lambda x: "Carrier: {}<br>Month: {}/{}<br>Pax: {}".format(
    #                 x["dom_op_al_code"], x["travel_date"].month, x["travel_date"].year, x["pax"]
    #             ),
    #             axis=1,
    #         )
    #     else:
    #         df["det_text"] = df.apply(
    #             lambda x: "Carrier: {}<br>Date: {}<br>Pax: {}".format(x["dom_op_al_code"], x["travel_date"], x["pax"]), axis=1
    #         )
    #     return df

    # def set_layout(self, fig):
    #     fig.update_layout(
    #         width=600,
    #         height=250,
    #         autosize=False,
    #         margin=dict(l=0, r=0, b=0, t=40, pad=4),
    #         paper_bgcolor=self.style.main_color,
    #         plot_bgcolor=self.style.main_color,
    #         title_font_family=self.style.font_family,
    #         title_font_size=self.style.title_font_size,
    #         title_x=0.5,
    #         legend_font_color=self.style.font_color,
    #         legend_font_size=self.style.legend_font_size,
    #         legend_font_family=self.style.font_family,
    #         legend_borderwidth=0,
    #         legend_title_text="Carriers",
    #         legend_title_font_color=self.style.font_color,
    #         legend_title_font_family=self.style.font_family,
    #         legend_title_font_size=self.style.legend_font_size,
    #         legend_title_side=self.style.legend_title_side,
    #         legend_bgcolor=self.style.main_color,
    #         legend_orientation=self.style.legend_orientation,
    #         legend_traceorder=self.style.legend_traceorder,
    #         legend_itemsizing=self.style.legend_item_sizing,
    #         legend_itemclick=self.style.legend_item_click,
    #         legend_itemdoubleclick=self.style.legend_item_double_click,
    #         legend_xanchor=self.style.legend_x_anchor,
    #         legend_yanchor=self.style.legend_y_anchor,
    #         legend_valign=self.style.legend_v_align,
    #         xaxis=dict(
    #             gridcolor=self.style.grid_color(), tickfont_size=18, tickfont_family=self.style.font_family, zeroline=False
    #         ),
    #         yaxis=dict(
    #             gridcolor=self.style.grid_color(), tickfont_size=18, tickfont_family=self.style.font_family, zeroline=False
    #         ),
    #     )
    #     return fig

    def set_booking_curve_layout(self, fig, typ):
        agg_type = request.args.get("agg_type")

        fig.update_layout(
            paper_bgcolor=self.style.main_color,
            plot_bgcolor=self.style.main_color,
            title_font_family=self.style.font_family,
            title_font_size=23,
            title_x=0.5,
            legend_font_color=self.style.font_color,
            legend_font_size=12,
            legend_font_family=self.style.font_family,
            legend_borderwidth=0,
            legend_title_font_color=self.style.font_color,
            legend_title_font_family=self.style.font_family,
            legend_title_font_size=12,
            legend_title_side=self.style.legend_title_side,
            legend_bgcolor=self.style.main_color,
            legend_orientation=self.style.legend_orientation,
            legend_traceorder=self.style.legend_traceorder,
            legend_itemsizing=self.style.legend_item_sizing,
            legend_itemclick=self.style.legend_item_click,
            legend_itemdoubleclick=self.style.legend_item_double_click,
            legend_xanchor=self.style.legend_x_anchor,
            legend_yanchor=self.style.legend_y_anchor,
            legend_valign=self.style.legend_v_align,
            autosize=False,
            margin=dict(l=100, r=0, b=100, t=100, pad=4),
        )

        fig.update_layout(legend_title_text="Time Blocks" if agg_type == "day_time" else "Carriers")

        for e in fig.layout:
            if "xaxis" in e or "yaxis" in e:
                fig.layout[e]["gridcolor"] = self.style.grid_color(0.4)
                fig.layout[e]["tickfont_size"] = self.style.font_size("md")
                fig.layout[e]["tickfont_family"] = self.style.font_family
                fig.layout[e]["gridwidth"] = 0.25
                fig.layout[e]["zeroline"] = False
                if "xaxis" in e:
                    fig.layout[e]["showgrid"] = False
                fig.layout[e]["title"]["font"] = {"color": self.style.font_color, "family": self.style.font_family}

        for e in fig.layout["annotations"]:
            e["font"]["family"] = self.style.font_family
            e["font"]["size"] = self.style.font_size("lg")

        fig.update_layout(width=1200, height=600)

        for e in fig.layout:
            if "xaxis" in e or "yaxis" in e:
                fig.layout[e]["title"]["text"] = ""
            for e in fig.layout["annotations"]:
                e["font"]["family"] = self.style.font_family
                e["font"]["size"] = self.style.font_size("lg")
                e["text"] = self.handle_booking_curve_fig_text(e)

        fig.update_layout(
            annotations=list(fig.layout.annotations)
            + [
                go.layout.Annotation(
                    x=-0.075,
                    y=0.5,
                    font=dict(size=14, family=self.style.font_family),
                    showarrow=False,
                    text="Total Pax" if typ == "total" else "Pax Ratio (%)",
                    textangle=-90,
                    xref="paper",
                    yref="paper",
                ),
                go.layout.Annotation(
                    x=0.5,
                    y=-0.2,
                    font=dict(size=14, family=self.style.font_family),
                    showarrow=False,
                    text="Days to Departure",
                    xref="paper",
                    yref="paper",
                ),
            ]
        )

        return fig

    def handle_booking_curve_fig_text(self, e):
        curr_title = e["text"].split("=")
        if len(curr_title) > 1 and (curr_title[1].isnumeric()) and 1 <= int(curr_title[1]) <= 7:
            return Constants.IDX2WEEKDAY[int(curr_title[1])]
        return curr_title[0]

    def __handle_dow_curve(self, df, fig_args):
        agg_type = request.args.get("agg_type")

        if agg_type == self.DOW_BOOKING_CURVE_AGG:
            return {
                **fig_args,
                "category_orders": {"travel_day_of_week": sorted(df.travel_day_of_week.unique().tolist())},
                "facet_col": "travel_day_of_week",
            }

        return fig_args

    def __handle_week_time_curve(self, df, fig_args):
        agg_type = request.args.get("agg_type")
        if agg_type == self.WEEK_TIME_BOOKING_CURVE_AGG:
            return {
                **fig_args,
                "category_orders": {
                    "local_dep_time": sorted(df.local_dep_time.unique().tolist()),
                    "travel_day_of_week": sorted(df.travel_day_of_week.unique().tolist()),
                },
                "facet_col": "travel_day_of_week",
                "facet_row": "dom_op_al_code",
            }

        return fig_args

    def booking_curve_viz(self, df: pd.DataFrame):
        y = "pax_ratio" if request.args.get("val_type", "ratio") == "ratio" else "agg_pax"

        fig_args = {
            "x": "days_sold_prior_to_travel",
            "y": y,
            "color": "local_dep_time" if request.args.get("agg_type", "overall") == "day-of-week-time" else "dom_op_al_code",
            "hover_data": ["agg_pax", "pax_ratio"],
        }

        fig_args = self.__handle_dow_curve(df, fig_args)
        fig_args = self.__handle_week_time_curve(df, fig_args)
        df["days_sold_prior_to_travel"] *= -1
        df.sort_values("days_sold_prior_to_travel", inplace=True)
        fig = px.line(df, **fig_args, custom_data=["agg_pax", "pax_ratio"], render_mode="svg")

        fig.update_layout(
            **{
                "title_x": 0.5,
                "legend_traceorder": self.style.legend_traceorder,
                "legend_orientation": self.style.legend_orientation,
                "legend_title_side": self.style.legend_title_side,
                "legend_title_text": self.booking_curve_legend_title_text,
                "paper_bgcolor": self.style.main_color,
                "plot_bgcolor": self.style.main_color,
                "legend_font_color": self.style.font_color,
                "legend_title_font_color": self.style.font_color,
                "width": 1200,
                "height": 600,
                "legend_valign": "middle",
                "legend_borderwidth": 0,
                "legend_xanchor": "left",
                "legend_yanchor": "auto",
                "legend_itemsizing": "trace",
                "legend_itemclick": "toggle",
                "legend_itemdoubleclick": "toggleothers",
                "margin": {"l": 100, "r": 0, "b": 100, "t": 100, "pad": 4},
            }
        )
        fig.update_xaxes(showgrid=False)
        fig.update_traces(
            hovertemplate="<b>Passengers: </b>%{customdata[0]}<br><b>Cumulative Booked Ratio: </b>%{customdata[1]} <extra></extra>"
        )
        fig = self.__handle_booking_curve_line_style(fig)
        fig = self.__handle_booking_curve_grid_style(fig)
        fig = self.__handle_booking_curve_annotations_style(fig)
        return json.loads(fig.to_json())

    @property
    def booking_curve_legend_title_text(self):
        if request.args.get("agg_type") == self.WEEK_TIME_BOOKING_CURVE_AGG:
            return "Time Blocks"
        return "Carriers"

    def __handle_booking_curve_line_style(self, fig: px.line) -> px.line:
        carrier_color_map = self.get_carrier_color_map()
        for e in fig.data:
            e.line.color = carrier_color_map.get(e.name)
            e.line.width = 6
            e.line.smoothing = 1.3
            e.line.shape = "spline"
        return fig

    def __handle_booking_curve_grid_style(self, fig: px.line) -> px.line:
        for e in fig.layout:
            if "xaxis" in e or "yaxis" in e:
                fig.layout[e]["gridcolor"] = "rgba(174, 181, 191, 0.4)"
                fig.layout[e]["tickfont_size"] = self.style.font_size("md")
                fig.layout[e]["tickfont_family"] = self.style.font_family
                fig.layout[e]["gridwidth"] = 0.25
                fig.layout[e]["zeroline"] = False
                fig.layout[e]["title"]["font"] = {"color": self.style.font_color, "family": self.style.font_family}
                fig.layout[e]["title"]["text"] = ""

            if e == "xaxis":
                fig.layout[e]["showgrid"] = False

        return fig

    def __handle_booking_curve_annotations_style(self, fig: px.line) -> px.line:
        calc_type = request.args.get("val_type", "total-bookings")

        for e in fig.layout["annotations"]:
            text = e.text.split("=")[1]
            text = Constants.IDX2WEEKDAY.get(int(text)) if text.isnumeric() else text
            e["text"] = text
            e["font"]["family"] = self.style.font_family
            e["font"]["size"] = self.style.font_size(18)

        fig.update_layout(
            annotations=list(fig.layout.annotations)
            + [
                go.layout.Annotation(
                    x=-0.075,
                    y=0.5,
                    font=dict(size=14, family=self.style.font_family),
                    showarrow=False,
                    text="Total Pax" if calc_type == "total-bookings" else "Pax Ratio (%)",
                    textangle=-90,
                    xref="paper",
                    yref="paper",
                ),
                go.layout.Annotation(
                    x=0.5,
                    y=-0.2,
                    font=dict(size=14, family=self.style.font_family),
                    showarrow=False,
                    text="Days to Departure",
                    xref="paper",
                    yref="paper",
                ),
            ]
        )
        return fig
