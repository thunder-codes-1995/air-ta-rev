from dataclasses import dataclass, field
from typing import Callable, Dict, List, Literal, Union, cast

import pandas as pd
import plotly.graph_objects as go

from base.entities.carrier import Carrier
from base.helpers.theme import Gradient, Theme


def get_lf_line_color_map(data: pd.DataFrame, host_code) -> Dict[str, str]:
    ORANGE_SHADES = ["#FFA500", "#A36A00", "#754C00", "#9f2b00", "#DF362D", "#9F2B00", "#3B0918"]
    host_data = data[data.carrierCode == host_code]

    if host_data.empty:
        return {}

    unique_line_ids = host_data.lineId.unique().tolist()

    if len(unique_line_ids) > len(ORANGE_SHADES):
        diff = len(unique_line_ids) - len(ORANGE_SHADES)
        ORANGE_SHADES = [*ORANGE_SHADES, *["#FFFFFF"] * diff]

    return {line_id: ORANGE_SHADES[idx] for idx, line_id in enumerate(unique_line_ids)}


@dataclass
class LoadFactorFigure:
    host_code: str
    data: pd.DataFrame
    xaxis: str
    hover_text_generator: Callable
    order: Literal[-1, 1]
    line_name: str
    color: str = field(default="orange")

    def get(self) -> Union[go.Scatter, None]:
        lf_df = self.data[self.data.carrierCode == self.host_code]
        lf_df = lf_df[lf_df.lf != "-"]
        lf_df = lf_df[~lf_df.lf.isna()]

        if lf_df.empty:
            return None

        lf_df = lf_df.sort_values("fareAmount")
        lf_df = lf_df.drop_duplicates(self.xaxis)
        lf_df.lf = lf_df.lf.astype(int)
        lf_df = lf_df.sort_values(self.xaxis, ascending=self.order == 1)
        lf_df["hover_text"] = lf_df.apply(self.__hover_text, axis=1)

        return go.Scatter(
            x=lf_df[self.xaxis],
            y=lf_df["lf"].tolist(),
            line={"color": self.color, "smoothing": 1.3, "shape": "spline", "width": 1.5, "dash": "dash"},
            yaxis="y2",
            name=f"{self.line_name}",
            mode="lines+markers",
            text=lf_df["hover_text"],
            hoverinfo="text",
            marker=dict(
                line=dict(color=self.color, width=1),
                size=3,
                symbol="circle-open",
            ),
        )

    def __hover_text(self, row: pd.Series) -> str:
        return (
            self.hover_text_generator(row[self.xaxis], row["lf"])
            + f"<br /><br />Market<br />{row.origin}-{row.destination}<br /><br />Flight Number<br />{int(row.flt_num)}<br /><br />Last Updated<br />{row.lf_inserted_date_formatted}"
        )


@dataclass
class Color:
    host_code: str
    data: pd.DataFrame
    origin: List[str]
    destination: List[str]
    theme: Theme

    def map(self) -> Dict[str, str]:
        res = {}
        carriers_colors_map = Carrier(self.host_code).carrier_colors(self.origin, self.destination, self.theme)
        for carrier, g_df in self.data.groupby("carrierCode"):
            line_colors = g_df.lineId.unique().tolist()
            res.update(
                Gradient(self.theme).map(
                    cast(str, carriers_colors_map.get(cast(str, carrier))),
                    line_colors,
                    self.host_code,
                )
            )

        return res


@dataclass
class AttachHoverText:
    data: pd.DataFrame

    def attach(self) -> pd.DataFrame:
        """data to be displayed on hover"""

        def build_hover_text(row: pd.Series):
            flt_num = f"{row.flight_number} / {row.connecting_flight_keys}" if row.is_connecting else row.flight_number
            lf = f"{int(row.lf)}%" if row.lf != "-" else row.lf
            return f"Departing <br />{row['formatted_date']}\t\t\t{row['weekday']} {row['formatted_time']}<br /><br />Market<br />{row['marketOrigin']}-{row['marketDestination']}<br /><br />Flight Number<br />{flt_num}<br /><br />Total {row.currency_symbol}<br />{f'{int(row.fareAmount):,}'}<br /><br />LF <br />{lf}<br /><br />LF Last Updated <br />{row.lf_inserted_date_formatted}<br /><br />DTD<br />{row['dtd']}"

        df = self.data.copy()
        df["flight_number"] = df.flightKey.apply(lambda val: val.split("|")[0])
        df["text"] = df.apply(build_hover_text, axis=1)
        return df
