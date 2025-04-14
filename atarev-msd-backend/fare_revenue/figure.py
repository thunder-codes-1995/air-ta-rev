import calendar
import json

import numpy as np
import pandas as pd
import plotly.graph_objects as GO
from flask import request
from plotly.colors import hex_to_rgb, make_colorscale, n_colors
from plotly.subplots import make_subplots

from base.constants import Constants
from base.entities.currency import Currency
from base.figure import BaseFigure


class FareRevenueFigure(BaseFigure):
    def fare_trends_viz(self, df, typ):
        currencies = Currency(df.currency.unique().tolist()).symbol
        xaxis = df.travel_date.sort_values().unique().tolist()
        fig = GO.Figure()
        for al, color in self.get_carrier_color_map().items():
            curr = df.query("{} == '{}'".format("dom_op_al_code", al)).sort_values(by="travel_date", ascending=True)

            if curr.empty:
                continue
            curr["text_desc"] = curr["blended_fare"].astype(int)
            curr["text_desc"] = curr.apply(
                lambda x: f"Carrier : {x['dom_op_al_code']}<br>Date: {str(x['travel_date'])}<br>Average Fare: ${str(x['text_desc'])}",
                axis=1,
            )
            # if typ == Constants.AGG_VIEW_MONTHLY:
            #     curr["travel_date"] = curr["travel_date"].apply(lambda v: calendar.month_abbr[v.month] + str(v.year))

            fig.add_trace(
                GO.Scatter(
                    x=xaxis,
                    y=curr["blended_fare"].tolist(),
                    showlegend=True,
                    fill="tozeroy" if typ == Constants.AGG_VIEW_MONTHLY else None,
                    line=dict(
                        color=color,
                        dash="solid",
                        smoothing=1.3,
                        shape="spline",
                        width=6,
                    ),
                    name=al,
                    hoverinfo="text",
                    hovertext=curr["text_desc"].tolist(),
                    opacity=0.8,
                    mode="lines",
                    visible=True,
                    legendgroup=al,
                )
            )
        # fig.update_xaxes(tickvals=df["travel_date"].unique().tolist())

        fig.update_layout(
            xaxis=dict(
                # type="date",
                title="Date",
                title_font_color="rgb(174, 181, 191)",
                title_font_size=13,
                title_font_family="Open Sans",
                gridcolor="rgb(248, 235, 247)",
                tickfont_size=12,
                tickfont_family="Open Sans",
                zeroline=False,
                showgrid=False,
            ),
            yaxis=dict(
                title=f"Average Fare ({','.join(currencies)})",
                title_font_color="rgb(174, 181, 191)",
                title_font_size=13,
                title_font_family="Open Sans",
                gridcolor="rgb(248, 235, 247)",
                tickfont_size=12,
                tickfont_family="Open Sans",
                zeroline=False,
                showgrid=False,
            ),
            paper_bgcolor="rgb(32, 32, 68)",
            plot_bgcolor="rgb(32, 32, 68)",
            title_font_family="Open Sans",
            title_font_size=23,
            title_x=0.5,
            legend_font_color="rgb(174, 181, 191)",
            legend_font_size=12,
            legend_font_family="Open Sans",
            legend_borderwidth=0,
            legend_title_text="Carriers",
            legend_title_font_color="rgb(174, 181, 191)",
            legend_title_font_family="Open Sans",
            legend_title_font_size=12,
            legend_title_side="top",
            legend_bgcolor="rgb(32, 32, 68)",
            legend_orientation="v",
            legend_traceorder="normal",
            legend_itemsizing="trace",
            legend_itemclick="toggle",
            legend_itemdoubleclick="toggleothers",
            legend_xanchor="left",
            legend_yanchor="auto",
            legend_valign="middle",
            width=1150,
            height=390,
            autosize=False,
            margin=dict(l=0, r=0, b=0, t=0, pad=4),
        )

        if typ == Constants.AGG_VIEW_MONTHLY:
            for e in fig.layout:
                if "xaxis" in e or "yaxis" in e:
                    fig.layout[e]["gridcolor"] = "rgb(248, 235, 247)"
                    fig.layout[e]["tickfont_size"] = 13
                    fig.layout[e]["tickfont_family"] = "Open Sans"
                    fig.layout[e]["zeroline"] = False
                    fig.layout[e]["showgrid"] = False
                for e in fig.layout["annotations"]:
                    e["font"]["family"] = "Open Sans"
                    e["font"]["size"] = 18

        fig.layout.template = None
        return json.loads(fig.to_json())

    def fare_histogram_viz(self, df, fig_title):
        fig = GO.Figure()
        view_option = request.args.get("view_opt", Constants.CUMULATIVE_VIEW)
        nbins = 1000 if view_option == Constants.CUMULATIVE_VIEW else 50
        colors_dict = self.get_carrier_color_map(is_gradient=True)
        airlines_name = df["dom_op_al_code"].iloc[0]
        color = colors_dict[airlines_name][0]

        fig.add_trace(
            GO.Histogram(
                x=df["blended_fare"].tolist(),
                cumulative_enabled=(view_option == Constants.CUMULATIVE_VIEW),
                nbinsx=nbins,
                # marker=dict(color="rgba(0, 255, 233, 0.75)")
                marker=dict(color=color),
            )
        )

        fig.update_layout(
            title_text=fig_title,
            xaxis=dict(
                tickfont_size=12,
                tickfont_family="Open Sans",
                zeroline=False,
                showgrid=False,
            ),
            yaxis=dict(
                tickfont_size=12,
                tickfont_family="Open Sans",
                zeroline=False,
                showgrid=False,
            ),
            paper_bgcolor="rgb(32, 32, 68)",
            plot_bgcolor="rgb(32, 32, 68)",
            title_font_family="Open Sans",
            title_font_size=23,
            title_x=0.5,
            height=390,
            width=590,
            autosize=False,
            margin=dict(l=0, r=0, b=0, t=50, pad=4),
        )

        fig = self.update_layouts(fig)
        fig.layout.template = None
        return json.loads(fig.to_json())

    def fare_dow_revenue_viz(self, df):
        fig = GO.Figure()
        colors = self.get_carrier_color_map()
        for _, yearmo_df in df.groupby(["travel_year", "travel_month"]):
            added_count = 0
            for al, al_df in yearmo_df.groupby("dom_op_al_code"):
                al_df = al_df.merge(
                    pd.DataFrame({"travel_day_of_week": [1, 2, 3, 4, 5, 6, 7]}),
                    on="travel_day_of_week",
                    how="outer",
                )
                al_df["blended_rev"] = al_df["blended_rev"].fillna(0)
                al_df = al_df.sort_values(by="travel_day_of_week", ascending=True)
                al_df["travel_day_of_week"] = al_df["travel_day_of_week"].map(Constants.IDX2WEEKDAY)

                fig.add_trace(
                    GO.Bar(
                        x=al_df["travel_day_of_week"],
                        y=al_df["blended_rev"],
                        name=al,
                        marker=dict(color=colors.get(al, "#ffffff")),
                        showlegend=True,
                        hovertemplate="%{x}, %{y}<extra></extra>",
                    )
                )
                added_count += 1

        fig.update_layout(
            barmode="group",
            xaxis=dict(
                gridcolor="rgb(248, 235, 247)",
                tickfont_size=12,
                tickfont_family="Open Sans",
                zeroline=False,
                showgrid=False,
            ),
            yaxis=dict(
                gridcolor="rgb(248, 235, 247)",
                tickfont_size=12,
                tickfont_family="Open Sans",
                zeroline=False,
                showgrid=False,
            ),
            paper_bgcolor="rgb(32, 32, 68)",
            plot_bgcolor="rgb(32, 32, 68)",
            title_font_family="Open Sans",
            title_font_size=23,
            title_x=0.5,
            legend_font_color="rgb(174, 181, 191)",
            legend_font_size=12,
            legend_font_family="Open Sans",
            legend_borderwidth=0,
            legend_title_text="Carriers",
            legend_title_font_color="rgb(174, 181, 191)",
            legend_title_font_family="Open Sans",
            legend_title_font_size=12,
            legend_title_side="top",
            legend_bgcolor="rgb(32, 32, 68)",
            legend_orientation="v",
            legend_traceorder="normal",
            legend_itemsizing="trace",
            legend_itemclick="toggle",
            legend_itemdoubleclick="toggleothers",
            legend_xanchor="left",
            legend_yanchor="auto",
            legend_valign="middle",
            autosize=False,
            margin=dict(l=0, r=0, b=0, t=20, pad=4),
            width=590,
            height=390,
        )
        for e in fig.layout:
            if "xaxis" in e or "yaxis" in e:
                fig.layout[e]["gridcolor"] = "rgb(248, 235, 247)"
                fig.layout[e]["tickfont_size"] = 13
                fig.layout[e]["tickfont_family"] = "Open Sans"
                fig.layout[e]["zeroline"] = False
                fig.layout[e]["showgrid"] = False
                if "xaxis" in e:
                    fig.layout[e]["categoryorder"] = "array"
                    fig.layout[e]["categoryarray"] = [
                        "Mon",
                        "Tue",
                        "Wed",
                        "Thu",
                        "Fri",
                        "Sat",
                        "Sun",
                    ]
            for e in fig.layout["annotations"]:
                e["font"]["family"] = "Open Sans"
                e["font"]["size"] = 18
        fig.layout.template = None
        return json.loads(fig.to_json())

    def rbd_elastic_viz(self, df: pd.DataFrame, carrier: str, main_al: str, competitor: str):
        fig = make_subplots(rows=1, cols=1, subplot_titles=(carrier,))
        fig.add_trace(self.__draw_rbd_heatmap(df, carrier), row=1, col=1)

        start_color = self.get_carrier_color_map(is_gradient=True)[main_al][0]
        end_color = self.get_carrier_color_map(is_gradient=True)[competitor][1]
        colors = self.__get_rbd_color_range(df, start_color, end_color)
        fig = self.__adjust_rbd_heatmap_colorscale(fig, colors)
        fig = self.style_figure(fig, layout={"showlegend": False, "title_x": 0.5})
        fig = self.update_layouts(fig)
        fig.update_layout(coloraxis_showscale=False)
        return json.loads(fig.to_json())

    def __draw_rbd_heatmap(self, df: pd.DataFrame, carrier: str) -> GO.Heatmap:
        targeted_df = df[df.index.get_level_values(1) == carrier]
        return GO.Heatmap(
            z=targeted_df.values,
            x=[e[1] for e in targeted_df.columns.values],
            y=targeted_df.index.to_frame().reset_index(drop=True)["RBD"],
            type="heatmap",
            name="Price Elasticity",
            coloraxis="coloraxis",
        )

    def __get_rbd_color_range(self, df: pd.DataFrame, start, end):
        rows_count, cols_count = df.shape[0], df.shape[1]
        vals_count = rows_count * cols_count
        if not vals_count:
            return []
        return self.n_colors([str(hex_to_rgb(start)), str(hex_to_rgb(end))], vals_count)

    def __adjust_rbd_heatmap_colorscale(self, fig, colors):
        if not colors:
            return []
        custom_scale = make_colorscale(colors)
        fig.update_layout(coloraxis={"colorscale": custom_scale})
        fig.update_coloraxes(
            colorbar=dict(
                title="PED",
                title_font_size=13,
                title_font_color=self.style.font_color,
                title_font_family=self.style.font_family,
                tickfont=dict(family=self.style.font_family, size=12, color=self.style.font_color),
            )
        )
        return fig

    def class_mix_viz(self, df_sums, df_mix, df_avg, yearmo_colors, MAIN_COMP, curr_yearmo):
        return_figs = []
        # NOTE for now both host and main comp have the same value "PY"
        colors_dict = self.get_carrier_color_map(is_gradient=True)
        for al in [request.user.carrier, request.args.get("main_competitor")]:
            curr_year_op = df_sums.query("{} == '{}'".format("dom_op_al_code", al))
            rbd_order = df_avg.query("{} == '{}'".format("dom_op_al_code", al)).sort_values(by="blended_fare", ascending=True)
            if type(al) == type(None) or curr_year_op.empty or rbd_order.empty:
                fig = GO.Figure(data=None, layout=None)
            else:
                fig = make_subplots(rows=1, cols=2, specs=[[{"type": "bar"}, {"type": "sunburst"}]])  # go.Figure()

                fig.update_layout(
                    template="simple_white",
                    barmode="stack",
                    xaxis=dict(
                        gridcolor="rgb(248, 235, 247)",
                        tickfont_size=12,
                        tickfont_family="Open Sans",
                        zeroline=False,
                        showgrid=False,
                    ),
                    yaxis=dict(
                        gridcolor="rgb(248, 235, 247)",
                        tickfont_size=12,
                        tickfont_family="Open Sans",
                        zeroline=False,
                        showgrid=False,
                    ),
                )

                rbd_grouped = curr_year_op.groupby(["seg_class", "rbkd"])

                # get colors for bar plot. the higher price the darker color.
                airlines_name = curr_year_op["dom_op_al_code"].iloc[0]
                airlines_color_range = [str(hex_to_rgb(color)) for color in colors_dict[airlines_name]]
                bar_colors = n_colors(*airlines_color_range, len(rbd_order), colortype="rgb")
                rbd_order["color"] = bar_colors

                for _, rbd_row in rbd_order.iterrows():
                    if (rbd_row["seg_class"], rbd_row["rbkd"]) in rbd_grouped.groups:
                        plot_df = rbd_grouped.get_group((rbd_row["seg_class"], rbd_row["rbkd"]))
                        plot_df = (
                            pd.DataFrame(
                                [(tmp_al, tmp_day) for tmp_al in [al] for tmp_day in range(1, 8)],
                                columns=["dom_op_al_code", "travel_day_of_week"],
                            )
                            .merge(
                                plot_df,
                                on=["dom_op_al_code", "travel_day_of_week"],
                                how="left",
                            )
                            .sort_values(by="travel_day_of_week", ascending=True)
                        )
                        plot_df["percentage"] = plot_df["percentage"].fillna(0)
                        plot_df["rbkd"] = plot_df["rbkd"].fillna(rbd_row["rbkd"])
                        plot_df["day_text"] = plot_df["day_text"].map(
                            {
                                "Monday": "Mon",
                                "Tuesday": "Tue",
                                "Wednesday": "Wed",
                                "Thursday": "Thu",
                                "Friday": "Fri",
                                "Saturday": "Sat",
                                "Sunday": "Sun",
                            }
                        )

                        fig.add_trace(
                            GO.Bar(
                                x=plot_df.day_text.tolist(),
                                y=["%.2f" % (prc * 100) for prc in plot_df.percentage.tolist()],
                                name=rbd_row["rbkd"],
                                text=rbd_row["rbkd"],
                                textposition="none",
                                # marker_color=yearmo_colors[(curr_yearmo[0], curr_yearmo[1], rbd_row["seg_class"])][
                                #     (al, rbd_row["rbkd"])],
                                marker_color=rbd_row["color"],
                                visible=True,
                                customdata=[rbd_row["rbkd"]] * 7,  # 7 days
                                hovertemplate="%{x}, %{y}<br>%{customdata}<extra></extra>",
                            ),
                            row=1,
                            col=1,
                        )

                seg_conv_mix = {
                    "Y": "Economy Class",
                    "C": "Business Class",
                    "F": "First Class",
                    "W": "Premium Economy Class",
                }
                # seg_conv_mix = {'Premium/Full Economy Class': 'Prem Economy', 'Business Class': 'Business', 'Discount Economy Class': 'Disc Economy', 'Other Classes': 'Other'}
                curr_seg_pie_df = df_mix.query("{} == '{}'".format("dom_op_al_code", al))
                curr_classes = curr_seg_pie_df.drop_duplicates(["seg_class"])
                curr_parents = curr_classes["seg_class"].tolist()
                curr_par_vals = curr_classes.pax_sum.tolist()
                curr_par_price = (
                    curr_seg_pie_df.groupby("seg_class").agg({"blended_fare": "mean", "pax_sum": "first"}).reset_index()
                )

                airlines_name = curr_seg_pie_df["dom_op_al_code"].iloc[0]
                airlines_avg_price = np.average(curr_par_price["blended_fare"], weights=curr_par_price["pax_sum"])
                airlines_passenger_count = curr_par_price["pax_sum"].sum()

                curr_par_price_dict = dict(zip(curr_par_price["seg_class"], curr_par_price["blended_fare"]))
                curr_par_price = [curr_par_price_dict[par] for par in curr_parents]

                labels = [seg_conv_mix.get(e, "") for e in curr_parents] + curr_seg_pie_df["rbkd"].tolist()
                values = curr_par_vals + curr_seg_pie_df["pax"].tolist()
                text = ["$%.0f" % (price) for price in curr_par_price + curr_seg_pie_df["blended_fare"].tolist()]
                parents = [airlines_name] * len(curr_parents) + [
                    seg_conv_mix.get(e, "") for e in curr_seg_pie_df["seg_class"].tolist()
                ]

                # add airlines hover data to be shown in the outer most box
                labels += [airlines_name]
                values += [airlines_passenger_count]
                text += [f"${int(airlines_avg_price)}"]
                parents += [""]  # airlines doesn't have a parent

                customdata = np.stack((labels, values, text, parents), axis=-1)

                # get colors for each box, the higher fare the darker color
                prices = [int(t[1:]) for t in text]
                price_color_df = pd.DataFrame({"price": prices}).sort_values("price")
                price_color_df["color"] = n_colors(*airlines_color_range, len(price_color_df), colortype="rgb")
                colors = price_color_df.sort_index()["color"].to_list()

                fig.add_trace(
                    GO.Treemap(
                        labels=labels,
                        parents=parents,
                        values=values,
                        insidetextfont=dict(color="white", size=12, family="Fabriga"),
                        text=text,
                        # marker=dict(colors=["rgba(10, 10, 10, 10)"] * len(curr_parents) +
                        #                    [yearmo_colors[(curr_yearmo[0], curr_yearmo[1], s_c)][(al, rbd)] for s_c, rbd
                        #                     in zip(curr_seg_pie_df["seg_class"].tolist(),
                        #                            curr_seg_pie_df["rbkd"].tolist())]),
                        marker_colors=colors,
                        branchvalues="total",
                        customdata=customdata,
                        hovertemplate="%{customdata[0]}<br>Pax: %{customdata[1]}<br>%{customdata[2]}<extra></extra>",
                    ),
                    row=1,
                    col=2,
                )

                fig.update_layout(
                    height=470,
                    width=1200,
                    showlegend=True,
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0)",
                    autosize=False,
                    margin=dict(l=50, r=0, b=30, t=0, pad=4),
                )

            for e in fig.layout:
                if "xaxis" in e or "yaxis" in e:
                    fig.layout[e]["tickfont_size"] = 10
                    fig.layout[e]["tickfont_family"] = "Open Sans"
                    fig.layout[e]["tickangle"] = 0
                    fig.layout[e]["zeroline"] = False
                    fig.layout[e]["showgrid"] = False
                    if "xaxis" in e:
                        fig.layout[e]["categoryorder"] = "array"
                        fig.layout[e]["categoryarray"] = [
                            "Mon",
                            "Tue",
                            "Wed",
                            "Thu",
                            "Fri",
                            "Sat",
                            "Sun",
                        ]
                for e in fig.layout["annotations"]:
                    e["font"]["family"] = "Fabriga"
                    e["font"]["size"] = 12

                fig.layout.template = None
            return_figs.append(json.loads(fig.to_json()))

        return return_figs

    def get_fare_revenue_trends_viz(self, df):
        fig = GO.Figure()
        colors = self.get_carrier_color_map()
        plot_df = df.sort_values(by="travel_date", ascending=True)
        for al in df["dom_op_al_code"].unique().tolist():
            curr = plot_df.query("{} == '{}'".format("dom_op_al_code", al))
            fig.add_trace(
                GO.Bar(
                    x=curr["travel_date"].tolist(),
                    y=curr["blended_rev"].tolist(),
                    name=al,
                    # NOTE : none existing values should be added
                    marker=dict(color=colors.get(al, "#144aff")),
                    hovertemplate="%{x}, %{y}<extra></extra>",
                )
            )

        fig.update_layout(
            barmode="group",
            xaxis=dict(
                type="date",
                gridcolor="rgb(248, 235, 247)",
                tickfont_size=12,
                tickfont_family="Open Sans",
                zeroline=False,
                showgrid=False,
            ),
            yaxis=dict(
                gridcolor="rgb(248, 235, 247)",
                tickfont_size=12,
                tickfont_family="Open Sans",
                zeroline=False,
                showgrid=False,
            ),
            paper_bgcolor="rgb(32, 32, 68)",
            plot_bgcolor="rgb(32, 32, 68)",
            title_font_family="Open Sans",
            title_font_size=23,
            title_x=0.5,
            legend_font_color="rgb(174, 181, 191)",
            legend_font_size=12,
            legend_font_family="Open Sans",
            legend_borderwidth=0,
            legend_title_text="Carriers",
            legend_title_font_color="rgb(174, 181, 191)",
            legend_title_font_family="Open Sans",
            legend_title_font_size=12,
            legend_title_side="top",
            legend_bgcolor="rgb(32, 32, 68)",
            legend_orientation="v",
            legend_traceorder="normal",
            legend_itemsizing="trace",
            legend_itemclick="toggle",
            legend_itemdoubleclick="toggleothers",
            legend_xanchor="left",
            legend_yanchor="auto",
            legend_valign="middle",
            height=390,
            width=590,
            autosize=False,
            margin=dict(l=0, r=0, b=0, t=20, pad=4),
        )
        for e in fig.layout:
            if "xaxis" in e or "yaxis" in e:
                fig.layout[e]["gridcolor"] = "rgb(248, 235, 247)"
                fig.layout[e]["tickfont_size"] = 13
                fig.layout[e]["tickfont_family"] = "Open Sans"
                fig.layout[e]["zeroline"] = False
                fig.layout[e]["showgrid"] = False
            for e in fig.layout["annotations"]:
                e["font"]["family"] = "Open Sans"
                e["font"]["size"] = 18

        fig.layout.template = None
        return json.loads(fig.to_json())
