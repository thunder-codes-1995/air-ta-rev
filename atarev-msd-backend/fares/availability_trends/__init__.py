from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Union

import pandas as pd

from base.helpers.user import User
from fares.availability_trends.data import AVAttachFields, FareData
from fares.availability_trends.figure import Figure
from fares.availability_trends.forms import GetMinFareTrends
from fares.availability_trends.labels import AV_TRENDS_TABLE_LABELS
from fares.common.form import CHUNK_SIZE, FareForm
from fares.common.labels import get_host_stats_labels, get_stats_labels, get_table_labels
from fares.common.pagination import generate_meta
from fares.common.report import Report
from fares.common.statistics import AvailibilityTrendsStatistics, host_stats
from fares.common.table import Table
from fares.common.types import TableResp

cols = [
    "carrierCode",
    "maf",
    "departure_date",
    "weekday",
    "type",
    "market",
    "fltNum",
    "inFltNum",
    "deptTime",
    "lf",
    "duration",
    "is_connecting",
    "connecting_flight_keys",
    "cabinName",
    "classCode",
]


@dataclass
class AvTrends:
    form: Union[GetMinFareTrends, FareForm]
    user: User

    def table(self) -> TableResp:
        origin = self.form.get_origin(user=self.user)
        destination = self.form.get_destination(user=self.user)

        df = FareData(form=self.form, user=self.user).get()
        df = AVAttachFields(
            data=df,
            host_code=self.user.carrier,
            origin=origin,
            destination=destination,
            cabin=self.form.get_cabin(normalize=True),
            date_range=None,
            consider_flight_number=len(self.form.get_flight_keys()) > 0,
        ).get()

        df = df.sort_values("outboundDate", ascending=True)
        table_data = Table(
            user=self.user,
            data=df,
            origin=origin,
            destination=destination,
            theme=self.form.get_theme(),
            page=self.form.get_page(),
            chunck_size=CHUNK_SIZE,
            columns=cols,
            columns_order=self.form.columns_order(),
        ).get()

        if df.empty:
            return {"meta": generate_meta(), "data": [], "labels": AV_TRENDS_TABLE_LABELS}

        return {
            "data": self._json(table_data["data"]),
            "meta": generate_meta(table_data["current"], CHUNK_SIZE, df.shape[0], table_data["total_count"]),
            "labels": get_table_labels(AV_TRENDS_TABLE_LABELS),
        }

    def _json(self, data: pd.DataFrame) -> List[Dict[str, Union[Any]]]:
        return [
            {
                **item,
                "carrier": {"color": item["carrier_color"], "value": item["carrierCode"]},
                "maf": {"currency": item["maf"].split(" ")[0], "value": f'{int(item["maf"].split(" ")[1]):,}'},
            }
            for item in data.to_dict("records")
        ]

    def figure(self):
        empty_figure = {"data": [], "layout": {}}
        df = FareData(form=self.form, user=self.user).get()

        if df.empty:
            return {"fig": empty_figure, "stats": {"data": [], "layout": {}}}

        df = AVAttachFields(
            data=df,
            host_code=self.user.carrier,
            origin=self.form.get_origin(),
            destination=self.form.get_destination(),
            cabin=self.form.get_cabin(normalize=True),
            date_range=self.form.get_date_range(),
            consider_flight_number=len(self.form.get_flight_keys()) > 0,
        ).get()

        stats = AvailibilityTrendsStatistics(
            data=df,
            origin=self.form.get_origin(),
            destination=self.form.get_destination(),
            host_code=self.user.carrier,
            theme=self.form.get_theme(),
            date_range=self.form.get_date_range(),
            overview_only=self.form.is_overview(),
        )

        if not self.form.is_overview():
            return {
                "fig": Figure(df, self.form, self.user.carrier).get(),
                "stats": {"data": stats.get_stats(), "labels": get_stats_labels()},
                "host_stats": {
                    "data": host_stats(df, self.user.carrier, self.form.get_theme(), self.form.get_date_range()),
                    "lables": get_host_stats_labels(),
                },
            }

        return {
            "fig": empty_figure,
            "stats": {"data": stats.get_stats(), "labels": get_stats_labels()},
        }

    def report(self):
        df = FareData(form=self.form, user=self.user).get()

        df = AVAttachFields(
            data=df,
            host_code=self.user.carrier,
            origin=self.form.get_origin(),
            destination=self.form.get_destination(),
            cabin=self.form.get_cabin(normalize=True),
            date_range=None,
            consider_flight_number=len(self.form.get_flight_keys()) > 0,
        ).get()

        if df.empty:
            return []

        df = df.sort_values("outboundDate", ascending=True)
        return Report(
            data=df,
            header=[AV_TRENDS_TABLE_LABELS[col] for col in cols],
            file_name=f"av_trends_{self.user.carrier}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            columns=cols,
        ).get()
