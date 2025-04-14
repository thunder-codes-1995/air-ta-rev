from dataclasses import dataclass
from typing import List, Optional, Tuple

import pandas as pd

from base.helpers.cabin import CabinMapper
from base.helpers.datetime import Date
from fares.common.query import LoadFactorQuery
from flight_inventory.repository import FlightInventoryRepository

repo = FlightInventoryRepository()


@dataclass
class LFData:
    origin: List[str]
    destinaton: List[str]
    cabin: List[str]
    only_latest: bool
    host_code: str
    date_range: Optional[Tuple[int, int]] = None
    date: Optional[int] = None

    def get(self) -> pd.DataFrame:

        match = LoadFactorQuery(
            host_code=self.host_code,
            origin=self.origin,
            destination=self.destinaton,
            start_date=self.date_range[0] if self.date_range else None,
            end_date=self.date_range[1] if self.date_range else None,
            departure_date=self.date,
        ).query

        load_factor_data = repo.get_load_factor(match=match, only_latest=self.only_latest, cabin=self.cabin)
        load_factor_df = pd.DataFrame(load_factor_data)

        if load_factor_df.empty:
            return pd.DataFrame(
                [
                    {
                        "lf": "-",
                        "airline_code": "-",
                        "origin": "-",
                        "destination": "-",
                        "flt_num": "-",
                        "cabin": "-",
                        "date": "-",
                        "dept_date": "-",
                        "inserted_at": "-",
                        "inserted_date_formatted": "-",
                    }
                ]
            )

        load_factor_df["inserted_date_formatted"] = load_factor_df.inserted_date.apply(lambda val: Date(val).humanize())
        load_factor_df.sort_values("inserted_date", inplace=True, ascending=False)
        load_factor_df.lf.fillna(0, inplace=True)

        return load_factor_df[
            [
                "airline_code",
                "origin",
                "destination",
                "flt_num",
                "cabin",
                "date",
                "dept_date",
                "lf",
                "inserted_at",
                "inserted_date_formatted",
            ]
        ]


@dataclass
class AttachLF:
    data: pd.DataFrame
    host_code: str
    origin: List[str]
    destination: List[str]
    cabin: List[str]
    fares_mergeby: List[str]
    lf_mergeby: List[str]
    only_latest: bool
    date_range: Optional[Tuple[int, int]] = None
    date: Optional[int] = None

    def get(self) -> pd.DataFrame:
        dtd = lambda row: (Date(row.dept_date).date() - Date(row.date).date()).days
        cabin = lambda val: CabinMapper.normalize(val)
        df = self.data.copy()
        df["cabin_normalized"] = df.cabinName.apply(cabin)

        load_factor_df = LFData(
            origin=self.origin,
            destinaton=self.destination,
            cabin=self.cabin,
            only_latest=self.only_latest,
            host_code=self.host_code,
            date_range=self.date_range,
            date=self.date,
        ).get()

        if "dtd" in self.fares_mergeby and "dtd" in self.lf_mergeby:
            load_factor_df["dtd"] = load_factor_df.apply(dtd, axis=1)

        merged = df.merge(load_factor_df, how="left", left_on=self.fares_mergeby, right_on=self.lf_mergeby)
        merged["lf"] = merged["lf"].fillna("-")
        merged.inserted_at.fillna("-", inplace=True)
        merged.inserted_date_formatted.fillna("-", inplace=True)
        merged.rename(columns={"inserted_date_formatted": "lf_inserted_date_formatted"}, inplace=True)
        return merged
