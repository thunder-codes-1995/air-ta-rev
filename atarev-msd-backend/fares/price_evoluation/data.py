from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, Tuple, Union

import pandas as pd

from base.entities.exchange_rate import HistoricalExchangeRate
from base.helpers.datetime import Date
from base.helpers.user import User
from fares.availability_trends.forms import GetMinFareTrends
from fares.common import (
    AttachFields,
    currency_symbol,
    dt,
    formatted_conn_keys,
    formatted_duration,
    formatted_time,
    get_fares_mergeby,
    get_lf_mergeby,
)
from fares.common.data import AttachLF
from fares.common.form import FareForm
from fares.price_evoluation.forms import GetPriceEvolution
from fares.price_evoluation.query import PriceEvoluationQuery
from fares.repository import FareRepository

repo = FareRepository()
norm_date = lambda row: int((Date(row.outboundDate).date() + timedelta(days=row.dtd)).strftime("%Y%m%d"))


def find_closest_exchange_rate(rates: Dict[int, Dict[str, float]], date: int) -> Dict[str, float]:
    if not rates:
        return {}

    if len(rates) == 1:
        return rates.get(next(iter(rates.keys())))

    if rates.get(date):
        return rates[date]

    exchange_rates = None
    dates = deque(
        [
            int((Date(date).date() - timedelta(days=1)).strftime("%Y%m%d")),
            int((Date(date).date() + timedelta(days=1)).strftime("%Y%m%d")),
        ]
    )

    while not exchange_rates and bool(dates):
        prev, nxt = dates.popleft(), dates.popleft()

        if rates.get(prev):
            exchange_rates = rates[prev]
            break

        if rates.get(nxt):
            exchange_rates = rates[nxt]
            break

        dates.appendleft(int((Date(nxt).date() + timedelta(days=1)).strftime("%Y%m%d")))
        dates.appendleft(int((Date(prev).date() - timedelta(days=1)).strftime("%Y%m%d")))

    return exchange_rates or {}


@dataclass
class PEAttachFields:
    data: pd.DataFrame
    host_code: str
    form: GetPriceEvolution

    def attach(self) -> pd.DataFrame:
        df = self.__setup(self.data)
        df = AttachFields(df).attach(len(self.form.get_flight_keys()) > 0)
        df = AttachLF(
            data=df,
            host_code=self.host_code,
            origin=self.form.get_origin(),
            destination=self.form.get_destination(),
            cabin=self.form.get_cabin(normalize=True),
            fares_mergeby=get_fares_mergeby("price-ev"),
            lf_mergeby=get_lf_mergeby("price-ev"),
            only_latest=False,
            date=self.form.get_norm_date(),
        ).get()

        df = df.sort_values("fareAmount")
        merged = df.drop_duplicates(["lineId", "dtd"], keep="first")
        merged = merged.sort_values("outboundDate", ascending=False)
        return df

    def __setup(self, data: pd.DataFrame) -> pd.DataFrame:
        scrape_time = lambda val: datetime.strptime(Date(int(f"{val}"[0:8])).humanize(), "%Y-%m-%d").date()
        data.outboundDate = data.outboundDate.apply(dt)
        data.scrapeTime = data.scrapeTime.apply(scrape_time)
        return data


@dataclass
class FareData:
    form: Union[GetMinFareTrends, FareForm]
    user: User

    def get(self) -> pd.DataFrame:
        currency = self.form.get_ctype(self.user.carrier)
        df = pd.DataFrame(repo.aggregate(PriceEvoluationQuery(form=self.form, user=self.user).query))
        df = self.__handle_currency_exchang(df, to_currencies=currency)

        if df.empty:
            return df

        df = PEAttachFields(data=df, host_code=self.user.carrier, form=self.form).attach()
        df = self.__setup(df)
        return df

    def __setup(self, data: pd.DataFrame) -> pd.DataFrame:
        data["deptTime"] = data["time"].apply(formatted_time)
        data["fare"] = data.apply(currency_symbol, axis=1)
        data["time"] = data["time"].apply(formatted_time)
        data["duration"] = data["duration"].apply(formatted_duration)
        data["connecting_flight_keys"] = data["connecting_flight_keys"].apply(formatted_conn_keys)
        return data

    def __handle_currency_exchang(self, data: pd.DataFrame, to_currencies: str) -> pd.DataFrame:
        data["norm_date"] = data.apply(norm_date, axis=1)
        start_date, end_date = self.__get_date_range(data)
        base_currency = data.fareCurrency.unique().tolist()
        rates = HistoricalExchangeRate(base_currency=base_currency, currencies=[to_currencies]).rates(start_date, end_date)
        data["fareAmount"] = data.apply(lambda row: self.__exchange(row, to_currencies, rates), axis=1)
        data["fareCurrency"] = [to_currencies] * data.shape[0]
        return data

    def __exchange(self, row: pd.Series, currency: str, exchange_rates: Dict[int, Dict[str, float]]) -> int:
        _dt = row.norm_date
        rates = find_closest_exchange_rate(exchange_rates, _dt)
        ratio = rates.get(f"{row.fareCurrency}-{currency}") or 1
        return round(row.fareAmount * ratio)

    def __get_date_range(self, data: pd.DataFrame) -> Tuple[int, int]:
        s, e = int(data.norm_date.min()), int(data.norm_date.max())
        if s >= Date(datetime.now().date()).noramlize():
            s = Date(datetime.now().date() - timedelta(days=2)).noramlize()
        return s, e
