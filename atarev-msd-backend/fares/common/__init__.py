import calendar
from dataclasses import dataclass
from datetime import date, datetime
from typing import List, Literal

import pandas as pd

from base.entities.currency import Currency
from base.helpers.datetime import Date, Time
from base.helpers.duration import Duration

formatted_time = lambda val: Time(val).humanize()
formatted_in_fkey = lambda val: int(val) if val is not None else "-"
dt = lambda val: datetime.strptime(Date(val).humanize(), "%Y-%m-%d").date()
formatted_conn_keys = lambda val: val[:-1] if isinstance(val, str) else None
formatted_duration = lambda val: f"{Time(val).humanize().replace(':', '.')}h"
currency_symbol = lambda row: Currency.attach_currency(row["fareAmount"], row["currency_symbol"])
dtd = lambda row: (row["outboundDate"] - date(row["scrapeTime"].year, row["scrapeTime"].month, row["scrapeTime"].day)).days


def get_fares_mergeby(action: Literal["av-trends", "price-ev"]) -> List[str]:
    return (
        [
            "carrierCode",
            "marketOrigin",
            "marketDestination",
            "fltNum",
            "cabin_normalized",
            "departure_date",
        ]
        if action == "av-trends"
        else ["carrierCode", "marketOrigin", "marketDestination", "fltNum", "cabin_normalized", "dtd"]
    )


def get_lf_mergeby(action: Literal["av-trends", "price-ev"]) -> List[str]:
    return (
        [
            "airline_code",
            "origin",
            "destination",
            "flt_num",
            "cabin",
            "dept_date",
        ]
        if action == "av-trends"
        else [
            "airline_code",
            "origin",
            "destination",
            "flt_num",
            "cabin",
            "dtd",
        ]
    )


@dataclass
class AttachFields:
    data: pd.DataFrame

    def __add_formatted_date(self, df: pd.DataFrame):
        """format date Ex : Nov 22 2022"""
        df["formatted_date"] = df.outboundDate.apply(lambda val: datetime.strftime(val, "%b %d %Y"))

    def __add_text_weekday(self, df: pd.DataFrame):
        """get weekday as string : Sun, Fri"""
        df["weekday"] = df.outboundDate.apply(lambda val: calendar.day_abbr[val.weekday()])

    def __add_line_id(self, df: pd.DataFrame, consider_flight_number: bool):
        """
        line id corresponds to flights to be plotted for a single airline
        if there are multiple flights requested for a given airline there will be
        multiple line ids and therefore multiple lines
        """

        def handle(row: pd.Series) -> str:
            if consider_flight_number:
                return f"{row.carrierCode}-{row.marketOrigin}-{row.marketDestination}-{row.fltNum}"
            return f"{row.carrierCode}"

        df["lineId"] = df.apply(handle, axis=1)

    def __add_currency_col(self, df: pd.DataFrame):
        currency_map = Currency(df.fareCurrency.unique().tolist()).symbol
        df["currency_symbol"] = df.fareCurrency.map(currency_map)

    def __add_formatted_time(self, df: pd.DataFrame):
        df["formatted_time"] = df.time.apply(lambda val: Duration.format(val))

    def attach(self, consider_flight_number: bool) -> pd.DataFrame:
        if self.data.empty:
            return self.data

        df = self.data.copy()
        self.__add_formatted_date(df)
        self.__add_text_weekday(df)
        self.__add_line_id(df, consider_flight_number)
        self.__add_currency_col(df)
        self.__add_formatted_time(df)
        return df
