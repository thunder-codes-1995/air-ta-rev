from dataclasses import dataclass
from itertools import groupby
from typing import Dict, List, Optional, TypedDict

import pandas as pd

from base.entities.carrier import Carrier
from base.helpers.carrier import Handler
from base.helpers.theme import Theme
from base.helpers.user import User
from fares.common.form import ColumnOrder
from fares.common.pagination import Paginator


class TableData(TypedDict):
    data: pd.DataFrame
    current: Optional[int]
    total_count: Optional[int]


@dataclass
class Table:
    user: User
    data: pd.DataFrame
    origin: List[str]
    destination: List[str]
    theme: Theme
    page: Optional[int] = 1
    chunck_size: Optional[int] = 20
    columns: Optional[List[str]] = None
    columns_order: Optional[List[ColumnOrder]] = None

    def get(self) -> TableData:
        if self.data.empty:
            return {"data": pd.DataFrame({"carrier_color": [None]}), "current": None, "total_count": None}

        page_data = Paginator(page=self.page, chunk_size=self.chunck_size, data=self.data).get()
        data, current, _, _, page_count = page_data.values()
        colors = self._carrier_colors()

        if self.columns:
            data = data[self.columns]

        data["carrier_color"] = data["carrierCode"].map(colors)
        data = self.__sort_columns(data)
        return {"data": data, "current": current, "total_count": page_count}

    def _carrier_colors(self) -> Dict[str, str]:
        carriers = Carrier(self.user.carrier).bridge(self.origin, self.destination).get_city_based_makret().competitors()
        colors = Handler(carriers).colors(self.theme, self.user.carrier)
        return colors

    def __sort_columns(self, data: pd.DataFrame) -> pd.DataFrame:
        if not self.columns_order:
            return data

        for order, vals in groupby(self.columns_order, lambda obj: obj["sortOrder"]):
            data = data.sort_values([item["sortKey"] for item in vals], ascending=order == "asc")
        return data
