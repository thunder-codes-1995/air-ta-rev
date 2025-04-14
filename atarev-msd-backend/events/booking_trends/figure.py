import json
from dataclasses import dataclass
from typing import Any, Dict, Literal

import pandas as pd
import plotly.graph_objects as go

from base.entities.market import Bridge
from events.booking_trends.form import AggType, BookingTrendsForm


@dataclass
class PaxBookingTrendsFigure:
    pax_data: pd.DataFrame
    form: BookingTrendsForm
    host_code: str

    def __hover_text(self, row: pd.Series, agg_type: Literal["monthly", "daily"]) -> str:
        if agg_type == AggType.MONTHLY.value:
            return f"Carrier: {row.carrier_code}<br>Month: {row.travel_month}/{row.str_travel_date}<br>Pax: {row.pax}"
        return f"Carrier: {row.carrier_code}<br>Date: {row.str_travel_date}<br>Pax: {row.pax}"

    def __event_hover_text(self, row: pd.DataFrame) -> str:
        if pd.isna(row.formatted_end_date):
            return ""

        start_date, end_date = row.formatted_dept_date, row.formatted_end_date
        date = f"{start_date[:2]}-{end_date}" if start_date == end_date else f"{start_date}-{end_date}"

        return f"{row.country_code} - {row.event_name} - {date}"

    def add_events(self, figure: go.Figure, events_data: pd.DataFrame) -> None:

        for dept_date, g_df in events_data.groupby("dept_date"):

            figure.add_trace(
                go.Scatter(
                    line=dict(color="#ba22bd"),
                    x=[dept_date],
                    y=[g_df["pax"].iloc[0]],
                    text=g_df.event_hover_text.iloc[0],
                    showlegend=False,
                    hoverinfo="text",
                    mode="markers",
                    hoverlabel=dict(align="left", bgcolor="#9c00ed"),
                )
            )

    def get(self) -> Dict[str, Any]:

        fig = go.Figure()
        df = self.pax_data.copy()

        if df.empty:
            return {"data": [], "layout": {}}

        df["hover_text"] = df.apply(lambda row: self.__hover_text(row, self.form.get_agg_type()), axis=1)
        df["event_hover_text"] = df.apply(lambda row: self.__event_hover_text(row), axis=1)

        market = Bridge(
            self.host_code,
            self.form.get_origin(),
            self.form.get_destination(),
        ).get_city_based_makret()

        get_carrier_color_map = market.carrier_color_map(self.form.get_theme())

        for carrier, g_df in df.groupby("carrier_code"):
            fig.add_trace(
                go.Scatter(
                    x=g_df["dept_date"].tolist(),
                    y=g_df["pax"].tolist(),
                    text=g_df["hover_text"].tolist(),
                    showlegend=True,
                    line=dict(color=get_carrier_color_map.get(carrier), dash="solid", smoothing=1.3, shape="spline", width=6),
                    name=carrier,
                    hoverinfo="text",
                    opacity=1.0,
                    mode="lines",
                    legendgroup=carrier,
                )
            )

        self.add_events(figure=fig, events_data=df)
        fig.layout.template = None

        return json.loads(fig.to_json())
