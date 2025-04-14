from dataclasses import dataclass
from typing import List, Optional, Tuple, Union

import numpy as np
import pandas as pd

from base.entities.currency import Currency
from base.helpers.user import User
from fares.availability_trends.forms import GetMinFareTrends
from fares.availability_trends.query import AvTrendsMatchQuery
from fares.common import (
    AttachFields,
    currency_symbol,
    dt,
    dtd,
    formatted_conn_keys,
    formatted_duration,
    formatted_time,
    get_fares_mergeby,
    get_lf_mergeby,
)
from fares.common.data import AttachLF
from fares.common.form import FareForm
from fares.repository import FareRepository

repo = FareRepository()


@dataclass
class FareData:
    form: Union[GetMinFareTrends, FareForm]
    user: User

    def get(self) -> pd.DataFrame:

        query = AvTrendsMatchQuery(form=self.form, user=self.user).query
        data = pd.DataFrame(repo.aggregate(query))

        if data.empty:
            return pd.DataFrame(
                columns=[
                    "carrierCode",
                    "outboundDate",
                    "marketOrigin",
                    "marketDestination",
                    "fltNum",
                    "fareAmount",
                    "fareCurrency",
                    "time",
                    "cabinName",
                    "scrapeTime",
                    "duration",
                    "flightKey",
                    "currency_symbol",
                    "departure_date",
                    "weekday",
                    "market",
                    "is_connecting",
                    "connecting_flight_keys",
                    "inFltNum",
                    "classCode",
                ]
            )

        if self.form.should_convert_currency():
            currency = self.form.get_currency()
            rate = self.form.currency_conversion_rates(data.fareCurrency.unique().tolist()).get(currency, 1)

            data["fareAmount"] = round((data.fareAmount * rate), 2)
            data["fareCurrency"] = [currency] * data.shape[0]

        currency_map = Currency(data.fareCurrency.unique().tolist()).symbol
        data["currency_symbol"] = data.fareCurrency.map(currency_map)
        return data


@dataclass
class AVAttachFields:
    data: pd.DataFrame
    host_code: str
    origin: List[str]
    destination: List[str]
    cabin: List[str]
    consider_flight_number: bool
    date_range: Optional[Tuple[int, int]] = None

    def __setup(self) -> pd.DataFrame:
        """
        process DataFrame before attaching fields.
        """
        scrape_time = lambda val: val.date()
        df = self.data.copy()
        df["outboundDate"] = df["outboundDate"].apply(dt)
        df["scrapeTime"] = df["scrapeTime"].apply(scrape_time)
        df = AttachFields(df).attach(self.consider_flight_number)
        df["maf"] = df.apply(currency_symbol, axis=1)
        df["deptTime"] = df["time"].apply(formatted_time)
        df["duration"] = df["duration"].apply(formatted_duration)
        df = df.replace({np.nan: None})
        df["connecting_flight_keys"] = df["connecting_flight_keys"].apply(formatted_conn_keys)
        df["inFltNum"] = df["inFltNum"].apply(formatted_conn_keys)
        df["dtd"] = df.apply(dtd, axis=1)

        return df

    def __attach_lf(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Attach LF data to DataFrame.
        """
        df = AttachLF(
            data=df,
            host_code=self.host_code,
            origin=self.origin,
            destination=self.destination,
            cabin=self.cabin,
            fares_mergeby=get_fares_mergeby("av-trends"),
            lf_mergeby=get_lf_mergeby("av-trends"),
            only_latest=True,
            date_range=self.date_range,
        ).get()

        df = df.sort_values("fareAmount")
        merged = df.drop_duplicates(["lineId", "dtd", "departure_date"], keep="first")
        merged = merged.sort_values("outboundDate", ascending=False)
        return merged

    def get(self) -> pd.DataFrame:
        if self.data.empty:
            return self.data
        df = self.__setup()
        df = self.__attach_lf(df)
        return df
