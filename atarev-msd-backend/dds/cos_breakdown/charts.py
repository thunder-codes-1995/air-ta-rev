import json
from dataclasses import dataclass, field
from typing import Dict

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from airports.repository import AirportRepository
from base.entities.carrier import Carrier
from base.helpers.theme import Gradient, Theme
from utils.funcs import table_to_text

from ..forms import MsdGetCosBreakdown

repo = AirportRepository()


@dataclass
class Text:
    data: pd.DataFrame
    host_code: str

    def generate(self) -> Dict[str, Dict[str, str]]:
        if self.data.empty:
            return {}

        self.data["al_type"] = self.data.dom_op_al_code.apply(lambda val: "Host" if val == self.host_code else "Comp")
        return table_to_text(
            self.data,
            [("pax", "sum")],
            group_cols=["travel_year"],
            input_col_name="country_of_sale",
        )


@dataclass
class Figure:
    host_code: str
    data: pd.DataFrame
    form: MsdGetCosBreakdown
    carrier_color_map: Dict[str, str] = field(init=False, default=None)

    def __post_init__(self):
        if not self.data.empty:
            origin = self.form.get_origin()
            destination = self.form.get_destination()
            theme = self.form.get_theme()
            mp = Carrier(self.host_code).carrier_colors(origin, destination, theme)
            data = repo.get_country_airport_map([*self.form.get_origin(), *self.form.get_destination()])
            countries = [data[airport]["country_code"] for airport in data]
            self.data["cos_normalized"] = self.data.country_of_sale.apply(lambda val: val if val in countries else "Others")
            self.data.fillna("Others", inplace=True)
            self.carrier_color_map = mp

    def render(self, carrier_code: str):
        fig = make_subplots(
            rows=2,
            cols=2,
            specs=[[{"type": "domain"}, {"type": "bar"}], [{"type": "domain"}, {"type": "bar"}]],
            subplot_titles=("Total Passengers", "Others Breakdown", "Total Revenue", "Others Breakdown"),
            column_width=[0.2, 0.4],
            horizontal_spacing=0.15,
            vertical_spacing=0.15,
        )
        if self.data.empty:
            return json.loads(fig.to_json())

        targeted = self.data[self.data.dom_op_al_code == carrier_code]

        if targeted.empty:
            return json.loads(fig.to_json())

        fig.add_trace(
            COSBreakdownPie(carrier_code, targeted, self.form.get_theme()).render("pax", self.carrier_color_map),
            row=1,
            col=1,
        )
        fig.add_trace(
            COSBreakdownPie(carrier_code, targeted, self.form.get_theme()).render("blended_fare", self.carrier_color_map),
            row=2,
            col=1,
        )
        fig.add_trace(
            COSBreakdownBar(carrier_code, targeted, self.form.get_theme()).render("pax", self.carrier_color_map),
            row=1,
            col=2,
        )
        fig.add_trace(
            COSBreakdownBar(carrier_code, targeted, self.form.get_theme()).render("blended_fare", self.carrier_color_map),
            row=2,
            col=2,
        )

        fig.update_layout(
            margin=dict(l=0, r=0, b=0, t=60, pad=8),
            showlegend=False,
            autosize=False,
            height=508,
            width=512,
            bargap=0,
            title=dict(text=carrier_code, x=0.5),
        )
        return json.loads(fig.to_json())


@dataclass
class COSBreakdownPie:
    carrier_code: str
    data: pd.DataFrame
    theme: Theme

    def render(self, key: str, carrier_color_map: Dict[str, str]) -> go.Pie:
        grouped = self.data.groupby("cos_normalized", as_index=False).agg({"pax": "sum", "blended_fare": "sum"})
        mp = Gradient(self.theme).map(carrier_color_map[self.carrier_code], grouped.cos_normalized.unique().tolist())

        return go.Pie(
            labels=grouped.cos_normalized,
            hoverinfo="label+percent+value",
            values=grouped[key].astype(int).sort_values(ascending=True),
            marker=dict(colors=list(mp.values())),
            textfont_size=14,
            textfont_family="Fabriga Medium",
            textposition="outside",
            hole=0.5,
        )


@dataclass
class COSBreakdownBar:
    carrier_code: str
    data: pd.DataFrame
    theme: Theme

    def render(self, key: str, carrier_color_map: Dict[str, str]) -> go.Bar:
        targeted = self.data[self.data.cos_normalized == "Others"].sort_values(key, ascending=False)
        bars_num = 10
        colors = Gradient(self.theme).shades(2, carrier_color_map[self.carrier_code])
        colors *= bars_num

        figure = go.Bar(
            x=targeted.country_of_sale.iloc[:bars_num],
            y=targeted[key].astype(int).iloc[:bars_num],
            marker=dict(color=colors),
            showlegend=False,
            legendgroup="Others",
            hovertext=targeted.country_of_sale,
            hoverinfo="x+y",
        )

        figure.marker.line.width = 0
        return figure
