from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Union

import pandas as pd

from base.helpers.user import User
from fares.common.form import CHUNK_SIZE
from fares.common.labels import get_host_stats_labels, get_stats_labels, get_table_labels
from fares.common.pagination import generate_meta
from fares.common.report import Report
from fares.common.statistics import AvailibilityTrendsStatistics, host_stats
from fares.common.table import Table
from fares.common.types import TableResp
from fares.price_evoluation.data import FareData
from fares.price_evoluation.figure import Figure
from fares.price_evoluation.forms import GetPriceEvolution
from fares.price_evoluation.labels import PE_TABLE_LABELS
from fares.repository import FareRepository

repo = FareRepository()

cols = [
    "carrierCode",
    "fare",
    "dtd",
    "cabinName",
    "fltNum",
    "weekday",
    "lf",
    "time",
    "duration",
    "is_connecting",
    "inFltNum",
    "market",
    "type",
    "classCode",
]


@dataclass
class PE:
    form: GetPriceEvolution
    user: User

    def figure(self):
        df = FareData(user=self.user, form=self.form).get()

        if df.empty:
            return {"fig": {"data": [], "layout": {}}, "stats": {"data": [], "labels": {}}}

        fig = Figure(df, self.user.carrier, self.form).get()
        stats = AvailibilityTrendsStatistics(
            data=df,
            origin=self.form.get_origin(),
            destination=self.form.get_destination(),
            host_code=self.user.carrier,
            theme=self.form.get_theme(),
            date_range=None,
            overview_only=False,
        )

        return {
            "fig": fig,
            "stats": {
                "data": stats.get_stats(),
                "labels": get_stats_labels(),
            },
            "host_stats": {
                "data": host_stats(df, self.user.carrier, self.form.get_theme()),
                "lables": get_host_stats_labels(),
            },
        }

    def table(self) -> TableResp:
        origin = self.form.get_origin(user=self.user)
        destination = self.form.get_destination(user=self.user)
        df = FareData(user=self.user, form=self.form).get()
        df = df.sort_values("dtd", ascending=False)

        if df.empty:
            return {"meta": generate_meta(), "data": [], "labels": PE_TABLE_LABELS}

        table_data = Table(
            user=self.user,
            data=df,
            origin=origin,
            destination=destination,
            theme=self.form.get_theme(),
            page=self.form.get_page(),
            chunck_size=CHUNK_SIZE,
            columns=cols,
        ).get()

        if df.empty:
            return {"meta": generate_meta(), "data": [], "labels": PE_TABLE_LABELS}

        return {
            "data": self._json(table_data["data"]),
            "meta": generate_meta(table_data["current"], CHUNK_SIZE, df.shape[0], table_data["total_count"]),
            "labels": get_table_labels(PE_TABLE_LABELS),
        }

    def _json(self, data: pd.DataFrame) -> List[Dict[str, Union[Any]]]:
        return [
            {
                **item,
                "carrier": {"color": item["carrier_color"], "value": item["carrierCode"]},
                "fare": {"currency": item["fare"].split(" ")[0], "value": f'{int(item["fare"].split(" ")[1]):,}'},
            }
            for item in data.to_dict("records")
        ]

    def report(self):
        df = FareData(user=self.user, form=self.form).get()
        df = df.sort_values("dtd", ascending=False)

        if df.empty:
            return []

        return Report(
            data=df,
            header=[PE_TABLE_LABELS.get(col) for col in cols if PE_TABLE_LABELS.get(col)],
            file_name=f"pe_{self.user.carrier}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            columns=cols,
        ).get()
