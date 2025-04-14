from dataclasses import asdict, dataclass, field
from typing import Dict, List, Union

import pandas as pd
from core.db import Collection
from core.stream import Stream

from .serializer import CabinSerializer, ClassSerializer, FareSerializer, MarketSerializer
from .source import FareStrucutreSource


@dataclass
class Fare:
    fare_basis: str
    surcharge_amt: int
    base_fare: int
    currency: str
    q: int
    q_currency: str
    yq: int
    yq_currency: str
    yr: int
    yr_currency: str
    fare_family: int
    first_ticketing_date: int
    last_ticketing_date: int
    observed_at: int
    travel_day_of_week: str
    country_of_sale: str
    footnote: str
    min_stay: int
    advance_purchase_days: int

    def __post_init__(self):
        FareSerializer(
            data={
                "fare_basis": self.fare_basis,
                "surcharge_amt": self.surcharge_amt,
                "base_fare": self.base_fare,
                "currency": self.currency,
                "q": self.q,
                "q_currency": self.q_currency,
                "yq": self.yq,
                "yq_currency": self.yq_currency,
                "yr": self.yr,
                "yr_currency": self.yr_currency,
                "fare_family": self.fare_family,
                "first_ticketing_date": self.first_ticketing_date,
                "last_ticketing_date": self.last_ticketing_date,
                "observed_at": self.observed_at,
                "travel_day_of_week": self.travel_day_of_week,
                "country_of_sale": self.country_of_sale,
                "footnote": self.footnote,
                "min_stay": self.min_stay,
                "advance_purchase_days": self.advance_purchase_days,
            }
        ).is_valid()


@dataclass
class Class:
    code: str
    fare_data: pd.DataFrame
    fares: List[Fare] = field(init=False)

    def __post_init__(self):
        ClassSerializer(data={"code": self.code}).is_valid()
        self.fares = self.__fares()

    def get_field(self, obj: Dict[str, Union[int, None]], key: str) -> float:
        return int(obj[key]) if type(obj[key]) is int else obj[key]

    def __fares(self):
        fares = []
        for obj in self.fare_data.to_dict("records"):
            surcharge_amt = self.get_field(obj, "surcharge_amt")
            base_fare = self.get_field(obj, "base_fare")
            q = self.get_field(obj, "q")
            yq = self.get_field(obj, "yq")
            yr = self.get_field(obj, "yr")
            first_ticketing_date = self.get_field(obj, "normalized_first_ticketing_date")
            last_ticketing_date = self.get_field(obj, "normalized_last_ticketing_date")
            observed_at = self.get_field(obj, "normalized_observed_at")
            min_stay = self.get_field(obj, "min_stay")
            advance_purchase_days = self.get_field(obj, "advance_purchase_days")

            fares.append(
                Fare(
                    fare_basis=obj["fare_basis"],
                    surcharge_amt=surcharge_amt,
                    base_fare=base_fare,
                    currency=obj["curr"],
                    q=q,
                    q_currency=obj["q_curr"],
                    yq=yq,
                    yq_currency=obj["yq_curr"],
                    yr=yr,
                    yr_currency=obj["yr_curr"],
                    fare_family=obj["fare_family"],
                    first_ticketing_date=first_ticketing_date,
                    last_ticketing_date=last_ticketing_date,
                    observed_at=observed_at,
                    travel_day_of_week=obj["t_dow"],
                    country_of_sale=obj["pos_country"],
                    footnote=obj["footnote"],
                    min_stay=min_stay,
                    advance_purchase_days=advance_purchase_days,
                )
            )
        return fares


@dataclass
class Cabin:
    code: str
    fare_date: pd.DataFrame = field(repr=False)
    classes: List[Class] = field(init=False)

    def __post_init__(self):
        CabinSerializer(data={"code": self.code}).is_valid()
        self.classes = self.__classes()

    def __classes(self):
        classes = []
        for (cls,), g_df in self.fare_date.groupby("class"):
            classes.append(Class(cls, g_df))
        return classes


@dataclass
class Market:
    airline_code: str
    origin: str
    destination: str
    fare_data: pd.DataFrame = field(repr=False)
    cabins: List[Cabin] = field(init=False)

    def __post_init__(self):
        MarketSerializer(data={"origin": self.origin, "destination": self.destination}).is_valid()
        self.cabins = self.__cabins()

    def __cabins(self):
        cabins = []
        for (cabin,), g_df in self.fare_data.groupby("cabin"):
            cabins.append(Cabin(cabin, g_df))
        return cabins


@dataclass
class Handler:
    date: int
    path: str

    def __post_init__(self):
        self.data = FareStrucutreSource(self.date, self.path).data

    def parse(self):
        stream = Stream(Collection.FS)

        for (origin, destination), g_df in self.data.groupby(["origin", "destination"]):
            market = Market("CY", origin, destination, g_df)
            stream.update(
                {
                    "date": self.date,
                    **asdict(market, dict_factory=lambda x: {k: v for (k, v) in x if type(v) is not pd.DataFrame}),
                },
                {"airline_code": "CY", "origin": origin, "destination": destination},
                upsert=True,
            )

        stream.update(upsert=True)
