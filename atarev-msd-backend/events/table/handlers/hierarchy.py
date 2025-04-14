from dataclasses import dataclass
from typing import Any, Dict, List, Literal, Tuple, Union

import numpy as np
import pandas as pd

from events.table.fields import PERIOD, CarrierSuffix, PeriodSuffix
from events.table.func import capacity, fare, load_factor, match

empty_event_obj = {
    f"start_month{PeriodSuffix.PAST.value}": None,
    f"str_start_date{PeriodSuffix.PAST.value}": None,
    f"str_end_date{PeriodSuffix.PAST.value}": None,
    f"start_dow{PeriodSuffix.PAST.value}": None,
    f"end_dow{PeriodSuffix.PAST.value}": None,
    "event_name": None,
    "city": None,
    f"start_month{PeriodSuffix.FUTURE.value}": None,
    f"str_start_date{PeriodSuffix.FUTURE.value}": None,
    f"str_end_date{PeriodSuffix.FUTURE.value}": None,
    f"start_dow{PeriodSuffix.FUTURE.value}": None,
    f"end_dow{PeriodSuffix.FUTURE.value}": None,
    "type": None,
    "sub_type": None,
    "is_loc_fixed": False,
    "is_date_fixed": False,
    "dept_date": None,
}


@dataclass
class Level:
    data: pd.DataFrame
    groupby: List[str]
    label: int
    host_code: str
    attach_fare_date: bool

    def get(self):

        if self.data.empty:
            return pd.DataFrame(
                columns=[
                    "cabin",
                    "country_code",
                    "market",
                    "segment",
                    "flt_num",
                    "dept_date",
                    f"lf{PeriodSuffix.PAST.value}",
                    f"cap{PeriodSuffix.PAST.value}",
                    f"avg_min_fare_host{PeriodSuffix.PAST.value}",
                    f"avg_min_fare_comp{PeriodSuffix.PAST.value}",
                    f"lf{PeriodSuffix.FUTURE.value}",
                    f"cap{PeriodSuffix.FUTURE.value}",
                    f"avg_min_fare_host{PeriodSuffix.FUTURE.value}",
                    f"avg_min_fare_comp{PeriodSuffix.FUTURE.value}",
                ]
            )

        past = Period(
            self.data,
            PERIOD.PAST.value,
            PeriodSuffix.PAST.value,
            groupby=self.groupby,
            host_code=self.host_code,
            attach_fare_date=self.attach_fare_date,
        ).get()

        future = Period(
            self.data,
            PERIOD.FUTURE.value,
            PeriodSuffix.FUTURE.value,
            groupby=self.groupby,
            host_code=self.host_code,
            attach_fare_date=self.attach_fare_date,
        ).get()

        merged = past.merge(future, on=self.groupby, suffixes=(PeriodSuffix.PAST.value, PeriodSuffix.FUTURE.value), how="outer")
        merged = merged.replace(np.nan, None)
        return merged


@dataclass
class Period:
    data: pd.DataFrame
    flag: Literal[-1, 1]
    suffix: Tuple[str, str]
    groupby: List[str]
    host_code: str
    attach_fare_date: bool

    def get(self) -> pd.DataFrame:
        data = self.data[self.data.flag == self.flag]

        if data.empty:
            return pd.DataFrame(
                columns=[
                    "cabin",
                    "country_code",
                    "market",
                    "segment",
                    "flt_num",
                    "dept_date",
                    "lf",
                    "cap",
                    f"avg_min_fare{CarrierSuffix.HOST.value}",
                    f"avg_min_fare{CarrierSuffix.COMP.value}",
                ]
            )

        host_df = data[data.carrier_code == self.host_code]
        comp_df = data[data.carrier_code != self.host_code]
        host_groupd = host_df.groupby(self.groupby)
        comp_grouped = comp_df.groupby(self.groupby)

        df: pd.DataFrame = load_factor(host_groupd, self.groupby)
        df = df.merge(capacity(host_groupd), on=self.groupby, how="outer")

        if self.attach_fare_date:
            df = df.merge(fare(host_groupd, comp_grouped, self.groupby), on=self.groupby, how="outer")

        return df


@dataclass
class Hierarchical:
    level0: pd.DataFrame
    level1: pd.DataFrame
    level2: pd.DataFrame
    level3: pd.DataFrame
    groupby0: List[str]
    groupby1: List[str]
    groupby2: List[str]
    groupby3: List[str]
    event_data: pd.DataFrame

    def get(self) -> List[Dict[str, Any]]:
        if self.level0.empty and self.event_data.empty:
            return [{**empty_event_obj, "data": []}]

        if self.level0.empty:
            return [{**event, "data": []} for event in self.event_data.to_dict("records")]

        loop = self.event_data.to_dict("records") if not self.event_data.empty else [empty_event_obj]
        groups = (self.groupby0, self.groupby1, self.groupby2, self.groupby3)
        levels = (self.level0, self.level1, self.level2, self.level3)
        hierarchy = self.__build(groups, levels, (0, 1, 1, 2))
        return [{**event, **hierarchy[0]} for event in loop]

    def __build(
        self,
        groups: Tuple[List[str], ...],
        levels: Tuple[pd.DataFrame, ...],
        offsets: Tuple[int, int, int, int],
        level: int = 0,
        query: Union[str, None] = None,
    ) -> List[Dict[str, Any]]:

        res = []
        if level == len(levels) - 1:
            cols = list(set(levels[level].columns.tolist()[offsets[level] :]))
            df = levels[level].query(query)[cols]
            df["level"] = [level + 1] * df.shape[0]
            return df.to_dict("records")

        if query:
            data = levels[level].query(query)
        else:
            data = levels[level]

        for g, g_df in data.groupby(groups[level]):
            cols = list(set(g_df.columns.tolist()[offsets[level] :]))
            target = g_df[cols]

            for item in target.to_dict("records"):
                q = match(groups[level + 1], (g,) if type(g) is str else g)
                res.append({"level": level + 1, **item, "data": self.__build(groups, levels, offsets, level + 1, q)})

        return res
