from dataclasses import dataclass
from typing import List

import pandas as pd

from base.helpers.user import User
from fares.keys.forms import GetFlightKeys
from fares.keys.query import GroupedKeys
from fares.repository import FareRepository

fare_repo = FareRepository()


@dataclass
class FlightKeys:
    user: User
    form: GetFlightKeys

    def get(self) -> List[str]:
        pipelines = GroupedKeys(form=self.form, user=self.user).query
        df = pd.DataFrame(fare_repo.aggregate(pipelines))
        if df.empty:
            return []
        df = df.drop_duplicates(["carrierCode", "fltNum"])
        df["fltNum"] = df.fltNum.astype(str)
        df["keys"] = df.apply(lambda row: row.carrierCode + row.fltNum, axis=1)
        selected = self._get_selected(df)
        df = self._handle_lookup(df)

        return list(set([*selected, *df["keys"].tolist()]))

    def _get_selected(self, data: pd.DataFrame) -> List[str]:
        if not self.form.get_selected(data):
            return []

        return list(filter(lambda key: key[0:2] in self.form.get_graph_carriers(), self.form.get_selected(data)))

    def _handle_lookup(self, data: pd.DataFrame) -> pd.DataFrame:
        if self.form.get_lookup():
            return data[data["keys"].str.contains(self.form.get_lookup(), case=False)]
        return data
