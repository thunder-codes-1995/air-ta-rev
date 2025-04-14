from calendar import monthrange
from datetime import date
from typing import List

import pandas as pd

from base.constants import Constants
from utils.funcs import from_int_to_datetime


def add_monthly_missing_dates(
    df: pd.DataFrame,
    start_date: date,
    end_date: date,
    columns: List[str] = [],
) -> pd.DataFrame:
    df["travel_date"] = df.apply(lambda x: date(x["travel_year"], x["travel_month"], 1), axis=1)
    dfs = []

    for grp, grp_df in df.groupby(["dom_op_al_code", "travel_year"]):
        carrier, year = grp

        # fill in missed dates (missed months)
        curr_all = pd.DataFrame(
            {
                "travel_date": [
                    date(year, month, 1)
                    for month in range(1, 13)
                    if date(year, month, 1) >= start_date and date(year, month, 1) <= end_date
                ]
            }
        )

        # fill in corresponding data for each created date
        curr_all["travel_year"] = [year] * len(curr_all)
        curr_all["travel_month"] = curr_all["travel_date"].apply(lambda x: x.month)
        curr_all["dom_op_al_code"] = [carrier] * len(curr_all)

        # merge the original df and the created one (thus having both the original values and the filled-in ones)
        curr_new = grp_df.merge(curr_all, on=["travel_date", "travel_year", "travel_month", "dom_op_al_code"], how="outer")[
            ["dom_op_al_code", "travel_year", "travel_month", *columns, "travel_date"]
        ]
        num_columns = curr_new.select_dtypes(include=["int64", "int32", "float64"]).columns
        curr_new[num_columns] = curr_new[num_columns].fillna(0)

        dfs.append(curr_new)

    return pd.concat(dfs, axis=0).sort_values(by="travel_date", ascending=True).reset_index(drop=True)


def add_daily_missing_dates(
    df: pd.DataFrame,
    start_date: date,
    end_date: date,
    columns: List[str] = [],
) -> pd.DataFrame:
    dfs = []

    df["travel_date"] = df.travel_date.apply(lambda val: from_int_to_datetime(val))

    for grp, grp_df in df.groupby(["dom_op_al_code", "travel_year", "travel_month"]):
        carrier, year, month = grp
        curr_max_days = monthrange(year, month)[1] + 1

        # fill in missed dates (missed days)
        curr_all = pd.DataFrame(
            {
                "travel_date": [
                    date(year, month, day)
                    for day in range(1, curr_max_days)
                    if date(year, month, day) >= start_date and date(year, month, day) <= end_date
                ]
            }
        )

        # fill in corresponding data for each created date
        curr_all["travel_year"] = [year] * len(curr_all)
        curr_all["travel_month"] = [month] * len(curr_all)
        curr_all["dom_op_al_code"] = [carrier] * len(curr_all)

        # merge the original df and the created one (thus having both the original values and the filled-in ones)
        curr_new = grp_df.merge(curr_all, on=["travel_date", "travel_year", "travel_month", "dom_op_al_code"], how="outer")[
            ["dom_op_al_code", "travel_date", *columns]
        ]
        num_columns = curr_new.select_dtypes(include=["int64", "int32", "float64"]).columns
        curr_new[num_columns] = curr_new[num_columns].fillna(0)
        dfs.append(curr_new)

    return pd.concat(dfs, axis=0).sort_values(by="travel_date", ascending=True).reset_index(drop=True)


def add_missing_dates(df: pd.DataFrame, start_date: str, end_date: str, columns: List[str] = [], typ=Constants.AGG_VIEW_MONTHLY):
    """fill-in all missed dates for a range of time based on carrier code,year, and month"""
    s_year, s_month, s_day = start_date.split("-")
    e_year, e_month, e_day = end_date.split("-")
    s_date = date(year=int(s_year), month=int(s_month), day=int(s_day))
    e_date = date(year=int(e_year), month=int(e_month), day=int(e_day))

    if typ == Constants.AGG_VIEW_MONTHLY:
        df = add_monthly_missing_dates(df, s_date, e_date, columns)
    else:
        df = add_daily_missing_dates(df, s_date, e_date, columns)
    df = df[(df["travel_date"] >= s_date) & (df["travel_date"] <= e_date)]
    return df


def return_date_parts_as_int(year: int, month: int, day: int):
    """takes a date string an returns it as integer"""
    _month = f"{month}" if month >= 10 else f"0{month}"
    _day = f"{day}" if day >= 10 else f"0{day}"
    return int(f"{year}{_month}{_day}")
