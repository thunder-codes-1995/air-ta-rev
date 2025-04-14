from dataclasses import dataclass
from datetime import date, timedelta
from typing import Dict, Optional, Tuple

import pandas as pd

from base.helpers.cabin import CabinMapper
from base.helpers.datetime import Date
from events.common import EventSetup
from events.common.query import LoadFactorQuery
from events.repository import EventRepository
from events.table.fields import PERIOD
from events.table.form import EventTableForm
from events.table.query import EventTableQuery, MinFareQuery
from fares.repository import FareRepository
from flight_inventory.repository import FlightInventoryRepository

inventory_repo = FlightInventoryRepository()
fare_repo = FareRepository()
event_repo = EventRepository()


@dataclass
class EventData:

    form: EventTableForm
    host_code: str

    def get(self) -> pd.DataFrame:

        dt = date(year=self.form.year.data, month=self.form.month.data, day=1).strftime("%Y-%m-%d")
        year, month, _ = dt.split("-")
        match = EventTableQuery(host_code=self.host_code, form=self.form).query
        match["start_month"] = {"$lte": int(month)}
        match["end_month"] = {"$gte": int(month)}
        c = event_repo.get_events(match=match)
        events_df = pd.DataFrame(c)

        if events_df.empty:
            return pd.DataFrame(
                columns=[
                    "city",
                    "country_code",
                    "dow",
                    "end_date",
                    "event_idx",
                    "event_name",
                    "event_year",
                    "start_date",
                    "start_month",
                    "id",
                    "str_start_date",
                    "str_end_date",
                    "date",
                    "start_dow",
                    "end_dow",
                    "flag",
                    "start_day_month",
                    "type",
                    "sub_type",
                ]
            )

        events_df.start_date = events_df.start_date.astype(int)
        events_df.end_date = events_df.end_date.astype(int)

        events_df = self.__setup(events_df)
        # display events only if they belong to selected month (handling date range 2024-04-29 -> 2024-05-09)
        events_df = events_df[events_df.date.str.contains(f"{year}-{month}")]
        events_df.country_code = events_df.country_code.fillna("-")
        return events_df.sort_values("date")

    def __setup(self, events_df: pd.DataFrame) -> pd.DataFrame:
        weekday = lambda val: Date(int(val)).weekday_abbr() if not pd.isna(val) else val
        period = lambda val: PERIOD.FUTURE.value if val >= nxt else PERIOD.PAST.value
        *_, nxt = self.form.get_years_range()

        df = events_df.copy()
        df = EventSetup.flatten(df, "type", "sub_type")
        df = EventSetup.fill(df, "str_start_date", "str_end_date")
        df = self.__fill(df)

        df["start_dow"] = df.start_date.apply(weekday)
        df["end_dow"] = df.end_date.apply(weekday)
        df["flag"] = df.event_year.apply(period)
        df["start_day_month"] = df.date.str[5:]

        return df[list(filter(lambda c: c != "categories", df.columns.tolist()))]

    def __fill(self, events_df: pd.DataFrame) -> pd.DataFrame:
        start_date = date(year=self.form.year.data, month=self.form.month.data, day=1)
        end_date = start_date + timedelta(days=self.form.get_days_count() - 1)
        rng = pd.date_range(start=start_date.strftime("%Y-%m-%d"), end=end_date.strftime("%Y-%m-%d"))
        date_df = pd.DataFrame({"str_start_date": rng, "date": rng})
        date_df.str_start_date = date_df.str_start_date.dt.strftime("%Y-%m-%d")
        date_df.date = date_df.date.dt.strftime("%Y-%m-%d")

        merged = pd.merge(
            left=events_df,
            right=date_df,
            how="outer",
            left_on=["str_start_date", "date"],
            right_on=["str_start_date", "date"],
        )

        return merged.sort_values("date")


@dataclass
class InventoryData:
    form: EventTableForm
    host_code: str
    date_range: Tuple[int, int]
    airport_country_map: Dict[str, str]

    def get(self) -> pd.DataFrame:

        match = LoadFactorQuery(
            self.host_code,
            self.form.get_origin(),
            self.form.get_destination(),
            self.form.is_country_market(),
            self.date_range,
        ).query

        lf_df = pd.DataFrame(
            inventory_repo.get_load_factor(
                match=match,
                extended=True,
                only_latest=False,
                cabin=self.form.get_cabin(),
            )
        )

        cols = [
            "origin",
            "destination",
            "flt_num",
            "dept_date",
            "cap",
            "total_booking",
            "cabin",
            "country_code",
            "norm_dept_dt",
            "market",
            "segment",
        ]

        if lf_df.empty:
            return pd.DataFrame(columns=cols)

        lf_df["norm_dept_dt"] = lf_df.dept_date.apply(lambda val: Date(val).noramlize())
        lf_df["country_code"] = lf_df.origin.map(self.airport_country_map)
        lf_df["segment"] = lf_df.apply(lambda row: f"{row.origin}-{row.destination}", axis=1)
        lf_df["cabin"] = lf_df.cabin.apply(lambda val: CabinMapper.humanize(val))

        lf_df = lf_df[(lf_df.cap != "-") & (lf_df.total_booking != "-")]
        lf_df.cap = lf_df.cap.astype(float)
        lf_df.total_booking = lf_df.total_booking.astype(float)
        lf_df = lf_df.drop_duplicates(["cabin", "dept_date", "flt_num"])

        return lf_df[cols]


@dataclass
class FareData:
    form: EventTableForm
    host_code: str
    date_range: Tuple[int, int]
    airport_country_map: Dict[str, str]
    comp_code: Optional[str] = None

    def get(self) -> pd.DataFrame:
        cols = [
            "flt_num",
            "dept_date",
            "fare",
            "currency",
            "country_code",
            "outbound_date",
            "carrier_code",
            "market",
            "segment",
            "norm_dept_dt",
            "cabin",
        ]

        query = MinFareQuery(
            form=self.form,
            date_range=self.date_range,
            host_code=self.host_code,
            comp_code=self.comp_code,
        ).query

        data = pd.DataFrame(fare_repo.aggregate(query))

        if data.empty:
            return pd.DataFrame(columns=cols)

        data["country_code"] = data.origin.map(self.airport_country_map)
        data["norm_dept_dt"] = data.outbound_date.tolist()
        data["cabin"] = data["cabin"].apply(lambda val: val.upper()[0:3] if val else val)
        data.cabin.fillna("-", inplace=True)

        return data[cols]


@dataclass
class Data:
    form: EventTableForm
    host_code: str
    date_range: Tuple[int, int]

    def get(self) -> pd.DataFrame:
        flag = lambda val: PERIOD.FUTURE.value if val >= nxt else PERIOD.PAST.value
        *_, nxt = self.form.get_years_range()
        mergeby = ["segment", "flt_num", "norm_dept_dt", "dept_date", "market", "country_code", "cabin"]
        airport_country_map = self.form.get_airport_country_map()

        inventory_data = InventoryData(
            form=self.form,
            host_code=self.host_code,
            date_range=self.date_range,
            airport_country_map=airport_country_map,
        ).get()

        fares_data = FareData(
            form=self.form,
            host_code=self.host_code,
            comp_code=self.form.get_main_competitor(self.host_code) if self.form.should_consider_stats() else None,
            date_range=self.date_range,
            airport_country_map=airport_country_map,
        ).get()

        merged = pd.merge(
            left=inventory_data,
            right=fares_data,
            left_on=mergeby,
            right_on=mergeby,
            how="outer",
        )

        merged.fillna(value={"carrier_code": "-"}, inplace=True)
        merged = merged[~merged.outbound_date.isna()]
        merged = merged.sort_values("outbound_date")
        merged["year"] = merged.dept_date.str[0:4].astype(int)
        merged["flag"] = merged.year.apply(flag)
        merged["dept_day_month"] = merged.dept_date.str[5:]
        merged.norm_dept_dt = merged.norm_dept_dt.astype(int)
        merged.outbound_date = merged.outbound_date.astype(int)

        return merged
