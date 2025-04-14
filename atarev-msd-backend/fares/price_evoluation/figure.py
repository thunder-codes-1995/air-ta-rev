import json
from dataclasses import dataclass

import pandas as pd
import plotly.graph_objects as go

from fares.common.figure import AttachHoverText, Color, LoadFactorFigure, get_lf_line_color_map
from fares.price_evoluation.forms import GetPriceEvolution


@dataclass
class Figure:
    data: pd.DataFrame
    host_code: str
    form: GetPriceEvolution

    def get(self):
        fig = go.Figure()
        df = self.data.copy()
        colors_map = Color(
            self.host_code,
            df,
            self.form.get_origin(),
            self.form.get_destination(),
            self.form.get_theme(),
        ).map()

        df.sort_values(["fareAmount"], inplace=True)
        df.drop_duplicates(["carrierCode", "dtd", "lineId"], keep="first", inplace=True)
        df.sort_values(["dtd"], inplace=True)

        df = AttachHoverText(df).attach()

        lf_color_map = get_lf_line_color_map(self.data, self.host_code)

        for _id, g_df in df.groupby("lineId"):
            fig.add_trace(
                go.Scatter(
                    x=g_df["dtd"],
                    y=g_df["fareAmount"],
                    line={
                        "color": colors_map[_id],
                        "smoothing": 1.3,
                        "shape": "spline",
                        "width": 1.5,
                    },
                    name=_id,
                    legendgroup=_id,
                    mode="lines+markers",
                    text=g_df["text"],
                    hoverinfo="text",
                    marker={"size": 3},
                )
            )

            lf_fig = LoadFactorFigure(
                host_code=self.host_code,
                data=g_df,
                xaxis="outboundDate",
                hover_text_generator=lambda value, lf: f"Departing<br />{value}<br /><br />LF<br />{lf}%",
                order=1,
                line_name=_id,
                color=lf_color_map.get(_id, "#FFFFFF"),
            ).get()

            if lf_fig is None:
                continue

            fig.add_trace(lf_fig)

        fig.update_layout(
            xaxis=dict(
                title="Days to Departure (DtD)",
                range=[max([df.dtd.max(), 4]) + 1, 0],
                tickfont_size=12,
                zeroline=False,
                showgrid=False,
            ),
            yaxis=dict(
                title=f"Minimum Fare ({df.currency_symbol.iloc[0]})",
                range=[0, df.fareAmount.max() + 100],
                zeroline=False,
                showgrid=False,
            ),
            yaxis2=dict(
                title="LF",
                overlaying="y",
                side="right",
                range=[0, 120],
                zeroline=False,
                showgrid=False,
            ),
            width=1000,
            height=450,
            margin=dict(l=0, r=0, b=0, t=30, pad=4),
            legend_borderwidth=0,
            autosize=False,
        )
        return json.loads(fig.to_json())
