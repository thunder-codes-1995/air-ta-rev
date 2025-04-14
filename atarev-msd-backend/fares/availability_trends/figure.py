import json
from dataclasses import dataclass

import pandas as pd
import plotly.graph_objects as go

from fares.availability_trends.forms import GetMinFareTrends
from fares.common.figure import AttachHoverText, Color, LoadFactorFigure, get_lf_line_color_map


@dataclass
class Figure:
    data: pd.DataFrame
    form: GetMinFareTrends
    host_code: str

    def get(self):
        fig = go.Figure()
        df = AttachHoverText(self.data).attach()
        color_map = Color(
            self.host_code,
            self.data,
            self.form.get_origin(),
            self.form.get_destination(),
            self.form.get_theme(),
        ).map()

        lf_color_map = get_lf_line_color_map(self.data, self.host_code)

        for _id, line_group in df.groupby("lineId"):
            fig.add_trace(
                go.Scatter(
                    x=line_group["outboundDate"],
                    y=line_group["fareAmount"],
                    line={
                        "color": color_map[_id],
                        "smoothing": 1.3,
                        "shape": "spline",
                        "width": 1.5,
                    },
                    name=_id,
                    legendgroup=_id,
                    mode="lines+markers",
                    text=line_group["text"],
                    hoverinfo="text",
                    marker=dict(
                        line=dict(color=color_map[_id], width=1),
                        size=3,
                        symbol="circle-open",
                    ),
                )
            )

            lf_fig = LoadFactorFigure(
                host_code=self.host_code,
                data=line_group,
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
            xaxis=dict(title="Travel Date", zeroline=False, showgrid=False),
            yaxis=dict(
                title=f"Minimum Fare ({','.join(self.data.currency_symbol.unique().tolist())})",
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
            height=450,
            width=900,
        )

        fig["layout"]["yaxis"]["range"] = [0, self.data.fareAmount.max() + 100]
        return json.loads(fig.to_json())
