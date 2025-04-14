from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Tuple, Union

import pandas as pd

from base.helpers.cabin import CabinMapper
from base.helpers.datetime import Date, Time
from rules.flights.evaluation.types import CompetitorCritera, Flight
from rules.types import Fct, FFact, Lg, Mkt


def first_or_empty(func):
    """
    Decorator that returns an empty DataFrame if the result is empty,
    otherwise returns the first row as a DataFrame.
    """

    def wrapper(*args, **kwargs):
        result: pd.DataFrame = func(*args, **kwargs)
        if result.empty:
            return result
        else:
            return result.head(1)

    return wrapper


def fare_fact(fares_df: pd.DataFrame, consider_load_factor: bool) -> Union[FFact, None]:
    if fares_df.empty:
        return None

    data = fares_df.iloc[0].to_dict()
    return {
        "carrierCode": data["carrier_code"],
        "fareAmount": data["fare"],
        "fareCurrency": data["currency"],
        "classCode": data["class"],
        "cabin": data["cabin"],
        "deptDate": data["departure_date"],
        "deptTime": data["departure_time"],
        "lf": int(data["lf"]) if consider_load_factor else None,
    }


def maf(host_fare: FFact, competitor_fare: FFact) -> Union[None, float]:
    if not host_fare or not competitor_fare:
        return None
    value = host_fare["fareAmount"] - competitor_fare["fareAmount"]
    return round(value, 2)


def leg(flight: Flight) -> Lg:
    return {
        "carrierCode": flight["carrier_code"],
        "flightNumber": flight["flt_num"],
        "arrivalDate": flight["arrival_date"],
        "arrivalDay": flight["arrival_day"],
        "arrivalMonth": flight["arrival_month"],
        "arrivalTime": flight["arrival_month"],
        "arrivalYear": flight["arrival_year"],
        "dayOfWeek": flight["dow"],
        "daysToDeparture": flight["dtd"],
        "deptDate": flight["departure_date"],
        "deptDay": flight["departure_day"],
        "deptMonth": flight["departure_month"],
        "deptTime": flight["departure_time"],
        "deptYear": flight["departure_year"],
        "destCode": flight["destination"],
        "originCode": flight["origin"],
    }


def market(flight: Flight) -> Mkt:
    return {
        "originCityCode": flight["origin"],
        "destCityCode": flight["destination"],
    }


@dataclass
class BestFare:

    flight: Flight
    fares: pd.DataFrame
    carrier_code: Optional[str] = None
    time_difference: Optional[Tuple[int, int]] = None

    @first_or_empty
    def get(self) -> pd.DataFrame:
        df = self.filter()
        df = df.sort_values("fare", ascending=True)
        return df

    def filter(self) -> pd.DataFrame:
        return self.fares


@dataclass
class HostBestFare(BestFare):

    flight: Flight
    fares: pd.DataFrame

    def filter(self) -> pd.DataFrame:
        df = self.fares[self.fares.carrier_code == self.flight["carrier_code"]]

        if df.empty:
            return pd.DataFrame(
                columns=[
                    "carrier_code",
                    "origin",
                    "destination",
                    "cabin",
                    "class",
                    "departure_date",
                    "departure_time",
                    "arrival_date",
                    "arrival_time",
                    "flt_num",
                    "fare",
                    "currency",
                    "is_connecting",
                    "op_code",
                    "mk_code",
                ]
            )

        df = df[
            (df.origin == self.flight["origin"])
            & (df.flt_num == self.flight["flt_num"])
            & (df.destination == self.flight["destination"])
            & (df.departure_time == self.flight["departure_time"])
            & (df.arrival_date == self.flight["arrival_date"])
            & (df.arrival_time == self.flight["arrival_time"])
            & (df.departure_date == self.flight["departure_date"])
        ]

        return df


@dataclass
class MainCompetitorBestFare:

    flight: Flight
    fares: pd.DataFrame
    host_dept_datetime: str
    carrier_code: str
    time_difference: Tuple[int, int]

    @first_or_empty
    def get(self) -> pd.DataFrame:
        host_datetime = datetime.strptime(self.host_dept_datetime, "%Y-%m-%d %H:%M")
        df = self.fares.copy()
        df["diff_hrs"] = df.apply(lambda row: self.__diff_hrs(row, host_datetime), axis=1)
        df = df[df.carrier_code == self.carrier_code]
        df = df[(df.diff_hrs >= self.time_difference[0]) & (df.diff_hrs <= self.time_difference[1])]
        df = df.sort_values(["fare", "diff_hrs"], ascending=True)

        return df

    def __diff_hrs(self, row: pd.Series, base_datetime: datetime) -> float:
        dt = f"{Date(row.departure_date).humanize()} {Time(row.departure_time).humanize()}"
        dt = datetime.strptime(dt, "%Y-%m-%d %H:%M")
        return abs((base_datetime - dt).total_seconds() / 3600)


@dataclass
class Fact:
    fares: pd.DataFrame
    flight: Flight
    cabin: str
    competitor_criteria: CompetitorCritera

    def get(self) -> Fct:
        host_fare = fare_fact(HostBestFare(self.flight, self.fares).get(), True)
        best_fare = fare_fact(BestFare(self.flight, self.fares).get(), False)

        if not host_fare:
            return {
                "hostFare": None,
                "mainCompetitorFare": None,
                "lowestFare": None,
            }

        host_dept_datetime = f"{Date(host_fare['deptDate']).humanize()} {Time(host_fare['deptTime']).humanize()}"
        competitor_fare = fare_fact(
            MainCompetitorBestFare(
                flight=self.flight,
                fares=self.fares,
                host_dept_datetime=host_dept_datetime,
                carrier_code=self.competitor_criteria["code"],
                time_difference=self.competitor_criteria["range"],
            ).get(),
            False,
        )

        facts = {
            "hostFare": host_fare,
            "mainCompetitorFare": competitor_fare,
            "lowestFare": best_fare,
            "leg": leg(self.flight),
            "market": market(self.flight),
            "cabin": {"cabinCode": self.cabin, "cabinCodeHumanized": CabinMapper.humanize(self.cabin)},
            "fares": {"maf": maf(host_fare, competitor_fare)},
        }

        return facts
