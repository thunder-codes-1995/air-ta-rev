from dataclasses import dataclass
from typing import Literal

import numpy as np
import pandas as pd

from events.table.fields import PeriodSuffix, is_date_fixed, is_loc_fixed

events_cols = [
    f"start_month{PeriodSuffix.PAST.value}",
    f"str_start_date{PeriodSuffix.PAST.value}",
    f"str_end_date{PeriodSuffix.PAST.value}",
    f"start_dow{PeriodSuffix.PAST.value}",
    f"end_dow{PeriodSuffix.PAST.value}",
    "event_name",
    "country_code",
    "date",
    "city",
    f"start_month{PeriodSuffix.FUTURE.value}",
    f"str_start_date{PeriodSuffix.FUTURE.value}",
    f"str_end_date{PeriodSuffix.FUTURE.value}",
    f"start_dow{PeriodSuffix.FUTURE.value}",
    f"end_dow{PeriodSuffix.FUTURE.value}",
    "type",
    "sub_type",
    "is_loc_fixed",
    "is_date_fixed",
]


rename = {
    f"city{PeriodSuffix.FUTURE.value}": "city",
    f"type{PeriodSuffix.FUTURE.value}": "type",
    f"sub_type{PeriodSuffix.FUTURE.value}": "sub_type",
}


@dataclass
class Event:
    event_data: pd.DataFrame

    def get(self) -> pd.DataFrame:

        if self.event_data.empty:
            return pd.DataFrame(columns=events_cols)

        df = self.__build_comparison()
        df[is_loc_fixed.value] = df.apply(self.__is_location_fixed, axis=1)
        df[is_date_fixed.value] = df.apply(self.__is_date_fixed, axis=1)
        df = df.rename(columns=rename)
        df: pd.DataFrame = df[events_cols]
        df = df.replace(np.nan, None)

        return df

    def __build_comparison(self) -> pd.DataFrame:
        past = self.__period(-1)
        future = self.__period(1)
        merged = pd.merge(
            left=past,
            right=future,
            left_on=["event_name", "country_code", "date"],
            right_on=["event_name", "country_code", "date"],
            suffixes=(PeriodSuffix.PAST.value, PeriodSuffix.FUTURE.value),
            how="outer",
        )
        return merged

    def __period(self, flag: Literal[-1, 1]):
        target = self.event_data[self.event_data.flag == flag]
        cols = [
            "city",
            "start_month",
            "str_start_date",
            "str_end_date",
            "start_dow",
            "end_dow",
            "event_name",
            "type",
            "sub_type",
            "country_code",
            "start_day_month",
            "date",
        ]

        if target.empty:
            pd.DataFrame(columns=cols)

        return target[cols].fillna("-")

    def __is_location_fixed(self, row: pd.Series) -> bool:
        if row.empty:
            return False

        if pd.isna(row[f"city{PeriodSuffix.PAST.value}"]) or pd.isna(row[f"city{PeriodSuffix.FUTURE.value}"]):
            return False
        return row[f"city{PeriodSuffix.PAST.value}"] == row[f"city{PeriodSuffix.FUTURE.value}"]

    def __is_date_fixed(self, row: pd.Series) -> bool:
        if row.empty:
            return False

        if pd.isna(row[f"str_start_date{PeriodSuffix.PAST.value}"]) or pd.isna(row[f"str_start_date{PeriodSuffix.FUTURE.value}"]):
            return False
        return row[f"start_day_month{PeriodSuffix.PAST.value}"] == row[f"start_day_month{PeriodSuffix.FUTURE.value}"]
