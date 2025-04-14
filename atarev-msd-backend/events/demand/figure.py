import json
from dataclasses import dataclass

import pandas as pd
import plotly.graph_objects as go

from base.entities.carrier import Carrier
from base.entities.market import CityBasedMarket
from base.helpers.carrier import Handler as Carrier
from events.demand.form import EventDemandForm


@dataclass
class Figure:
    form: EventDemandForm
    host_code: str
    year: int

    def get(self):
        return Bar(self.form, self.host_code, self.year).get()


@dataclass
class Line:
    form: EventDemandForm
    host_code: str

    def get(self):
        x = ["APR", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]
        y = [50, 58, 63, 70, 68, 75, 90]

        data = pd.DataFrame({"x": x, "y": y})
        origin = self.form.get_origin()[0]
        destination = self.form.get_destination()[0]
        carriers = CityBasedMarket(self.host_code, origin, destination).competitors()
        color_carrier_map = Carrier(carriers).colors(self.form.get_theme())

        fig = go.Figure()
        demand_trace = go.Scatter(
            x=data["x"],
            y=data["y"],
            mode="lines+markers",
            name="Demand Percentage",
            line={
                "color": color_carrier_map.get(self.host_code, "#ffffff"),
                "smoothing": 1.3,
                "shape": "spline",
                "width": 3,
            },
        )

        fig.add_trace(demand_trace)
        fig.update_layout(title="Demand Percentage by Booking Period", xaxis_title="Booking Period", yaxis_title="Demand")
        return json.loads(fig.to_json())


@dataclass
class Bar:
    form: EventDemandForm
    host_code: str
    year: int

    def get(self):
        x = ["Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
        y = [29, 106, 183, 405, 202, 345, 1002, 2256, 9657, 6319]
        avg_fares = [62, 75, 63, 92, 65, 95, 90, 116, 93, 160]
        curr = "$"

        origin = self.form.get_origin()[0]
        destination = self.form.get_destination()[0]
        carriers = CityBasedMarket(self.host_code, origin, destination).competitors()
        # color_carrier_map = Carrier(carriers).colors(self.form.get_theme())

        fig = go.Figure()
        fig.add_trace(
            go.Bar(
                x=x,
                y=y,
                name="data",
                marker=dict(color="#9C00ED"),
            )
        )

        fig.add_trace(
            go.Scatter(
                x=x,
                y=avg_fares,
                yaxis="y2",
                mode="lines+markers",
                marker=dict(color="#EC4252", size=40),  # Change color here
                name="Demand Percentage",
                line={
                    # "color": color_carrier_map.get(self.host_code, "#ffffff"),
                    "color": "#144aff",
                    "smoothing": 1.3,
                    "shape": "spline",
                    "width": 6,
                },
            )
        )

        fig.update_layout(
            yaxis2=dict(
                title="Secondary Axis",
                overlaying="y",  # Overlays on primary axis
                side="right",  # Position on right side
                range=[50, 180],  # Set range for secondary axis
            )
        )

        for mon, avg in zip(x, avg_fares):
            fig.add_annotation(
                x=mon,
                y=avg,
                yref="y2",
                text=f"<b>{curr}{avg}</b>",
                showarrow=False,
                font_size=13,
                font_color="#ffffff",
            )

        for mon, val in zip(x, y):
            fig.add_annotation(
                x=mon,
                y=avg,
                yref="y1",
                text=f"<b>{val:,}</b>",
                showarrow=False,
                font_size=16,
                font_color="#ffffff",
            )

        fig.update_layout(title=f"{self.year}", xaxis_title="Booking Period", yaxis_title="Demand")
        return json.loads(fig.to_json())
