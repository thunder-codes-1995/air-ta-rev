from typing import List, Union

import pandas as pd
from pandas.core.groupby.generic import DataFrameGroupBy

from events.common.fields import amf, capacity
from events.common.fields import load_factor as lf
from events.table.fields import CarrierSuffix


def as_df(from_field_name: Union[int, str], to_field_name: str) -> pd.DataFrame:

    def decorator(func):
        def wrapper(*args, **kwargs):
            result = func(*args, **kwargs)
            return result.reset_index().rename(columns={from_field_name: to_field_name})

        return wrapper

    return decorator


def load_factor(grouped_df: DataFrameGroupBy, groupby: List[str]) -> pd.DataFrame:
    cap = grouped_df.cap.sum().reset_index()
    tb = grouped_df.total_booking.sum().reset_index()
    merged = cap.merge(tb)
    merged = merged[(~merged.cap.isna()) & (merged.cap != 0)]
    # includ_cols = len(list(grouped_df.groups.keys())[0])
    includ_cols = [*groupby, lf.value]

    if merged.empty:
        return pd.DataFrame(columns=includ_cols)

    merged[lf.value] = merged.apply(lambda row: round((row.total_booking / row.cap) * 100), axis=1)
    return merged[includ_cols]


@as_df(0, capacity.value)
def capacity(grouped_df: DataFrameGroupBy) -> Union[pd.Series, None]:
    res: pd.Series = grouped_df.cap.sum()
    return res


@as_df("fare", amf.value)
def avg_min_fare(grouped_df: DataFrameGroupBy) -> Union[pd.Series, None]:
    res: pd.Series = round(grouped_df.fare.mean())
    return res


def fare(host_grouped_df: DataFrameGroupBy, comp_grouped_df: DataFrameGroupBy, groupby: List[str]) -> pd.DataFrame:
    host_avg_min_fare: pd.DataFrame = avg_min_fare(host_grouped_df)
    comp_avg_min_fare: pd.DataFrame = avg_min_fare(comp_grouped_df)

    return host_avg_min_fare.merge(
        comp_avg_min_fare,
        on=groupby,
        suffixes=(CarrierSuffix.HOST.value, CarrierSuffix.COMP.value),
        how="outer",
    )


def match(groupby: List[str], values: List[Union[str, int]]) -> str:
    conditions = [(f"{field}=='{value}'" if type(value) is str else f"{field}=={value}") for field, value in zip(groupby, values)]
    return " and ".join(conditions)
