import json

import pandas as pd
import plotly.graph_objects as go

from base.entities.currency import Currency
from base.figure import BaseFigure


class MarketShareFigure(BaseFigure):
    def market_share_viz(self, df):
        fig = go.Figure()
        colors = self.get_carrier_color_map()
        for al, curr in df.groupby("dom_op_al_code"):
            color = colors.get(al, "darkmagenta")
            curr = curr.sort_values(by="travel_date", ascending=True)
            if curr.empty:
                continue
            curr["mkt_share_conv"] = ["%.2f" % (ms * 100) for ms in curr.mkt_share.tolist()]
            curr["text_desc"] = curr["mkt_share_conv"]
            curr["text_desc"] = curr.apply(
                lambda x: "Carrier: {}<br>Date: {}<br>Market Share: %{}".format(
                    x["dom_op_al_code"], str(x["travel_date"]), x["text_desc"]
                ),
                axis=1,
            )

            fig.add_trace(
                go.Scatter(
                    x=curr["travel_date"].tolist(),
                    y=curr.mkt_share_conv.tolist(),
                    name=al,
                    hoverinfo="text",
                    hovertext=curr["text_desc"].tolist(),
                    opacity=0.8,
                    mode="lines",
                    line=dict(color=color, dash="dash", smoothing=1.3, shape="spline", width=6),
                    legendgroup=al,
                )
            )

        fig.update_layout(
            paper_bgcolor="rgb(32, 32, 68)",
            plot_bgcolor="rgb(32, 32, 68)",
            title_font_family="Open Sans",
            title_font_size=23,
            title_x=0.5,
            width=600,
            height=400,
            legend_font_color="rgb(174, 181, 191)",
            legend_font_size=12,
            legend_font_family="Open Sans",
            legend_title_text="Carriers",
            legend_title_font_size=13,
            legend_title_font_family="Open Sans",
            legend_title_font_color="rgb(174, 181, 191)",
            autosize=False,
            margin=dict(l=0, r=0, b=40, t=40, pad=4),
            xaxis=dict(
                type="date",
                title_font_color="rgb(174, 181, 191)",
                title_font_size=15,
                title_font_family="Open Sans",
                title_text="Date",
                gridcolor="rgb(248, 235, 247)",
            ),
            yaxis=dict(
                title_font_color="rgb(174, 181, 191)",
                title_font_size=15,
                title_font_family="Open Sans",
                gridcolor="rgb(248, 235, 247)",
                range=[0, 100],
                title_text="Market Share (%)",
            ),
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

    def fare_trends_viz(self, monthly_df: pd.DataFrame):
        currencies = ",".join(
            [Currency(curr).symbol for curr in monthly_df[~monthly_df.currency.isna()].currency.unique().tolist()]
        )

        fig = go.Figure()
        colors = self.get_carrier_color_map()
        for al, color in colors.items():
            curr = monthly_df.query("{} == '{}'".format("dom_op_al_code", al)).sort_values(by="travel_date", ascending=True)

            if curr.empty:
                continue
            curr["text_desc"] = curr["blended_fare"].astype(int)
            curr["text_desc"] = curr.apply(
                lambda x: "Carrier: {}<br>Date: {}<br>Average Fare: ${}".format(
                    x["dom_op_al_code"], str(x["travel_date"]), int(x["text_desc"])
                ),
                axis=1,
            )

            fig.add_trace(
                go.Scatter(
                    x=curr["travel_date"].tolist(),
                    y=curr["blended_fare"].tolist(),
                    showlegend=True,
                    fill="tozeroy",
                    line=dict(color=color, dash="solid", smoothing=1.3, shape="spline", width=6),
                    name=al,
                    hoverinfo="text",
                    hovertext=curr["text_desc"].tolist(),
                    opacity=0.8,
                    mode="lines",
                    visible=True,
                    legendgroup=al,
                )
            )

        fig.update_layout(
            xaxis=dict(
                type="date",
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
                title=f"Average Fare ({currencies})",
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

    def mshare_afare_viz(self, monthly_mkt):  # Market Share vs Average Fare
        fig = go.Figure()
        if monthly_mkt.empty:
            return json.loads(fig.to_json())

        monthly_mkt["mkt_share_conv"] = [round(ms * 100) for ms in monthly_mkt["mkt_share"].tolist()]

        monthly_mkt["text_desc"] = monthly_mkt.apply(
            lambda x: "Market Share: %{}<br>Total Passengers: {}".format(x["mkt_share_conv"], f"{x['pax']:,d}"), axis=1
        )
        size = monthly_mkt["pax"].tolist()

        """Preparing the colors"""
        light_colors, dark_colors = [], []
        for color_set in self.get_carrier_color_map(is_gradient=True, return_list=True):
            light_colors.append(color_set[0])
            dark_colors.append(color_set[1])

        fig.add_trace(
            go.Scatter(
                x=monthly_mkt.mkt_share_conv.tolist(),
                y=monthly_mkt["blended_fare"].tolist(),
                text=monthly_mkt["dom_op_al_code"].tolist(),
                hoverinfo="text",
                hovertext=monthly_mkt.text_desc.tolist(),
                textposition="middle center",
                opacity=0.8,
                mode="markers+text",
                marker=dict(
                    gradient=dict(type="radial", color=light_colors),
                    color=dark_colors,
                    line=dict(color=dark_colors, width=0),
                    size=size,
                    sizemode="area",
                    # sizeref=2.*max(size)/(55.**2),
                    sizemin=2,
                ),
                textfont=dict(color="rgba(255, 255, 255, 1)", family="Montserrat Semi-Bold", size=13),
            )
        )
        fig.update_layout(
            paper_bgcolor="rgb(32, 32, 68)",
            plot_bgcolor="rgb(32, 32, 68)",
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
                zerolinecolor="darkgray",
                autorange=False,
                range=[0, 100],
                title="Market Share (%)",
                title_font_color="rgb(174, 181, 191)",
                title_font_size=10,
                title_font_family="Open Sans",
                gridcolor="rgb(248, 235, 247)",
            ),
            yaxis=dict(
                zeroline=True,
                autorange=True,
                zerolinecolor="darkgray",
                # title="Average Fare ($)",
                # title_font_color="rgb(174, 181, 191)",
                # title_font_size=15,
                # title_font_family="Open Sans",
                gridcolor="rgb(248, 235, 247)",
            ),
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
