import json

import numpy as np
import pandas as pd
import plotly.graph_objects as GO
from _plotly_utils.colors import hex_to_rgb
from flask import request
from plotly.colors import n_colors
from plotly.subplots import make_subplots

from base.constants import Constants
from base.figure import BaseFigure

# FIXME: There is A LOT of hardcoded values such as fonts, colors, etc
# Is it possible to e.g. have a template object with those values defined once and use it in multiple places?


class DdsFigure(BaseFigure):
    def clearify(self, fig):
        if fig.get("layout") and fig["layout"]["template"]:
            del fig["layout"]["template"]
        return fig

    def product_map_viz(self, orig_long, dest_long, orig_lat, dest_lat, curr_orig, curr_dest):
        fig = GO.Figure()
        fig.add_trace(
            GO.Scattergeo(
                lon=[orig_long, dest_long],
                lat=[orig_lat, dest_lat],
                hoverinfo="text",
                text=[curr_orig, curr_dest],
                textfont_size=10,
                textfont_color="white",
                textfont_family="Open Sans",
                mode="markers+text+lines",
                showlegend=False,
            )
        )

        fig.update_layout(
            showlegend=False,
            width=500,
            height=250,
            margin=dict(l=0, r=0, b=0, t=0, pad=0),
            geo=dict(
                showland=True,
                showcountries=True,
                showocean=True,
                showlakes=False,
                showrivers=False,
                oceancolor="rgb(32, 32, 68)",
                landcolor="rgb(32, 32, 68)",
                resolution=110,
                showframe=False,
                framewidth=0,
                countrywidth=1,
                countrycolor="rgba(0, 255, 233, 0.75)",
                coastlinewidth=1,
                showcoastlines=False,
                lataxis=dict(
                    range=[min([orig_lat, dest_lat]) - 15, max([orig_lat, dest_lat]) + 15],
                ),
                lonaxis=dict(
                    range=[min([orig_long, dest_long]) - 30, max([orig_long, dest_long]) + 30],
                ),
            ),
        )

        return self.clearify(json.loads(fig.to_json()))

    def dist_mix_viz(self, df, picked_year, agg_type):
        colors_origin = self.get_carrier_color_map(is_gradient=True)
        seg_als = df["dom_op_al_code"].unique().tolist()
        num_rows = len(seg_als)
        graph_width = 278
        graph_height = graph_width * num_rows
        if agg_type == Constants.AGG_VIEW_MONTHLY:
            fig = make_subplots(cols=1, rows=num_rows, row_titles=seg_als)
        else:
            fig = make_subplots(cols=1, rows=num_rows, specs=[[{"type": "domain"}] for _ in range(num_rows)])

        idx = 2
        for carrier, sub_df in df.groupby("dom_op_al_code"):
            row_idx = idx
            if carrier == request.user.carrier:  # to make sure the host is at the beginning
                row_idx = 1
                idx -= 1
            len_values = len(sub_df["pax"].tolist())

            if len_values > 2:
                colors = n_colors(
                    f"rgb{hex_to_rgb(colors_origin.get(carrier, '#EF4351')[0])}",
                    f"rgb{hex_to_rgb(colors_origin.get(carrier, '#2F0E29')[1])}",
                    len_values,
                    colortype="rgb",
                )
            else:
                colors = colors_origin.get(carrier, ["#EF4351", "#2F0E29"])

            customdata = np.stack((sub_df.distribution_channel, sub_df["pax"]), axis=-1)
            fig.add_trace(
                GO.Pie(
                    labels=sub_df.distribution_channel.tolist(),
                    customdata=customdata,
                    values=sub_df["pax"].tolist(),
                    marker=dict(colors=colors),
                    title=carrier,
                    title_font_size=20,
                    title_font_family="Fabriga Regular",
                    sort=False,
                    hovertemplate="%{customdata[0][0]}<br>%{customdata[0][1]}<extra></extra>",
                ),
                col=1,
                row=row_idx,
            )
            idx += 1

        fig.update_traces(
            hole=0.5,
        )
        fig.update_layout(
            showlegend=False,
            paper_bgcolor="rgb(32, 32, 68)",
            plot_bgcolor="rgb(32, 32, 68)",
            title_font_family="Open Sans",
            title_font_size=23,
            title_x=0.5,
            width=graph_width,
            height=graph_height,
            autosize=False,
            margin=dict(l=10, r=10, b=10, t=10, pad=4),
        )
        if agg_type == Constants.AGG_VIEW_MONTHLY:
            fig = self.update_fig_layout(fig)

        return self.clearify(json.loads(fig.to_json()))

    def cabin_mix_viz(self, curr_df, picked_year, agg_type):
        colors_origin = self.get_carrier_color_map(is_gradient=True)
        seg_als = curr_df["dom_op_al_code"].unique().tolist()
        num_rows = len(seg_als)
        graph_width = 278
        graph_height = graph_width * num_rows

        if agg_type == Constants.AGG_VIEW_MONTHLY:
            fig = make_subplots(cols=1, rows=num_rows, row_titles=seg_als)
        else:
            fig = make_subplots(cols=1, rows=num_rows, specs=[[{"type": "domain"}] for _ in range(num_rows)])

        idx = 2
        for carrier, df in curr_df.groupby("dom_op_al_code"):
            row_idx = idx
            if carrier == request.user.carrier:  # to make sure the host is at the beginning
                row_idx = 1
                idx -= 1
            len_values = len(df["pax"].tolist())

            if len_values > 2:
                colors = n_colors(
                    f"rgb{hex_to_rgb(colors_origin.get(carrier, '#EF4351')[0])}",
                    f"rgb{hex_to_rgb(colors_origin.get(carrier, '#2F0E29')[1])}",
                    len_values,
                    colortype="rgb",
                )
            else:
                colors = colors_origin.get(carrier, ["#EF4351", "#2F0E29"])

            customdata = np.stack((df.seg_class, df["pax"]), axis=-1)
            fig.add_trace(
                GO.Pie(
                    labels=df.seg_class.tolist(),
                    customdata=customdata,
                    values=df["pax"].tolist(),
                    marker=dict(colors=colors),
                    title=carrier,
                    title_font_size=20,
                    title_font_family="Fabriga Regular",
                    sort=False,
                    hovertemplate="%{customdata[0][0]}<br>%{customdata[0][1]}<extra></extra>",
                ),
                col=1,
                row=row_idx,
            )
            idx += 1

        fig.update_traces(
            hole=0.5,
        )
        fig.update_layout(
            showlegend=False,
            paper_bgcolor="rgb(32, 32, 68)",
            plot_bgcolor="rgb(32, 32, 68)",
            title_font_family="Open Sans",
            title_font_size=23,
            title_x=0.5,
            width=graph_width,
            height=graph_height,
            autosize=False,
            margin=dict(l=10, r=10, b=10, t=10, pad=4),
        )
        if agg_type == Constants.AGG_VIEW_MONTHLY:
            fig = self.update_fig_layout(fig)
        return self.clearify(json.loads(fig.to_json()))

    def update_fig_layout(self, fig):
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
        return fig

    def bkg_mix_viz(self, curr_df, picked_year, agg_type):
        colors_origin = self.get_carrier_color_map(is_gradient=True)
        seg_als = curr_df["dom_op_al_code"].unique().tolist()
        num_rows = len(seg_als)
        graph_width = 278
        graph_height = graph_width * num_rows

        if agg_type == Constants.AGG_VIEW_MONTHLY:
            fig = make_subplots(cols=1, rows=num_rows, row_titles=seg_als)
        else:
            fig = make_subplots(cols=1, rows=num_rows, specs=[[{"type": "domain"}] for _ in range(num_rows)])

        idx = 2
        for carrier, df in curr_df.groupby("dom_op_al_code"):
            row_idx = idx
            if carrier == request.user.carrier:  # to make sure the host is at the beginning
                row_idx = 1
                idx -= 1
            len_values = len(df["pax"].tolist())

            if len_values > 2:
                colors = n_colors(
                    f"rgb{hex_to_rgb(colors_origin.get(carrier, '#EF4351')[0])}",
                    f"rgb{hex_to_rgb(colors_origin.get(carrier, '#2F0E29')[1])}",
                    len_values,
                    colortype="rgb",
                )
            else:
                colors = colors_origin.get(carrier, ["#EF4351", "#2F0E29"])

            customdata = np.stack((df.is_group, df["pax"]), axis=-1)
            fig.add_trace(
                GO.Pie(
                    labels=df.is_group.tolist(),
                    customdata=customdata,
                    values=df["pax"].tolist(),
                    marker=dict(colors=colors),
                    title=carrier,
                    title_font_size=20,
                    title_font_family="Fabriga Regular",
                    sort=False,
                    hovertemplate="%{customdata[0][0]}<br>%{customdata[0][1]}<extra></extra>",
                ),
                col=1,
                row=row_idx,
            )
            idx += 1

        fig.update_traces(
            hole=0.5,
        )
        fig.update_layout(
            showlegend=False,
            paper_bgcolor="rgb(32, 32, 68)",
            plot_bgcolor="rgb(32, 32, 68)",
            title_font_family="Open Sans",
            title_font_size=23,
            title_x=0.5,
            width=graph_width,
            height=graph_height,
            autosize=False,
            margin=dict(l=10, r=10, b=10, t=10, pad=4),
        )
        if agg_type == Constants.AGG_VIEW_MONTHLY:
            fig = self.update_fig_layout(fig)

        # fig.show()

        return self.clearify(json.loads(fig.to_json()))

    def empty_figure(self):
        return self.clearify(json.loads(GO.Figure(data=None, layout=None).to_json()))
