from dataclasses import dataclass
from enum import Enum
from typing import List, Tuple, Type, TypedDict

import pandas as pd

from base.entities.carrier import CityBasedMarket
from base.helpers.cabin import CabinMapper
from base.helpers.datetime import Date
from base.helpers.user import User
from events.calendar.fields import hover_group
from events.calendar.query import CalendarFareQuery, EventCalendarQuery
from events.common import EventSetup
from events.common.query import LoadFactorQuery
from events.repository import EventRepository
from fares.repository import FareRepository
from flight_inventory.repository import FlightInventoryRepository

from .form import EventCalendarForm

inventory_repo = FlightInventoryRepository()
fare_repo = FareRepository()
events_repo = EventRepository()


@dataclass
class EventData:
    form: EventCalendarForm
    host_code: str
    user: User

    def get(self) -> pd.DataFrame:
        match = EventCalendarQuery(host_code=self.host_code, form=self.form, user=self.user).query
        events_df = pd.DataFrame(events_repo.get_events(match=match))
        fixed_cols = ["date"]

        if self.form.should_consider_stats():
            fixed_cols += [
                "carrier_code_host",
                "fare_host",
                "currency_host",
                "carrier_code_comp",
                "fare_comp",
                "currency_comp",
                "lf",
            ]

        if not self.form.get_fields():
            cols = hover_group.values() + fixed_cols
        else:
            cols = self.form.get_fields() + fixed_cols

        if events_df.empty:
            return pd.DataFrame(columns=cols)

        events_df = self.__setup(events_df)
        events_df = self.__group_events(events_df)
        events_df = AttachValues(events_df, self.form, self.host_code).get()
        events_df.fillna("-", inplace=True)

        return events_df[cols]

    def __setup(self, event_form: pd.DataFrame) -> pd.DataFrame:
        df = event_form.copy()
        df = EventSetup.flatten(df, "type", "sub_type")
        df = EventSetup.fill(df, "str_start_date", "str_end_date")
        df = df.rename(columns={"str_start_date": "s_date", "str_end_date": "e_date"})
        return df

    def __group_events(self, event_df: pd.DataFrame) -> pd.DataFrame:
        """handle same events with diffent countries"""

        if event_df.empty:
            return event_df

        df = event_df.copy()
        df = df[["event_name", "type", "sub_type", "s_date", "e_date", "city", "country_code", "date"]]
        # group same events occurring on the same day together and concatenate country names into a single string separated by '-'
        event_grouped_df = (
            df.groupby(["event_name", "s_date", "e_date"])
            .apply(lambda _df: "-".join(list(_df.country_code.unique().tolist())))
            .reset_index()
            .rename(columns={0: "countries"})
        )

        df = event_grouped_df.merge(df)[["event_name", "s_date", "e_date", "type", "sub_type", "city", "countries", "date"]]
        df = df.drop_duplicates(["event_name", "type", "sub_type", "s_date", "e_date", "city", "countries", "date"])
        df.city.fillna("-", inplace=True)
        return df


@dataclass
class AttachLoadFactor:
    event_data: pd.DataFrame
    form: EventCalendarForm
    host_code: str

    def get(self) -> pd.DataFrame:
        df = self.event_data.copy()
        match = LoadFactorQuery(
            self.host_code,
            self.form.get_origin(),
            self.form.get_destination(),
            self.form.is_country_market(),
            self.__get_date_range(df),
        ).query

        lf_df = pd.DataFrame(
            inventory_repo.get_load_factor(
                match=match,
                cabin=self.__get_cabins(df),
                extended=True,
                only_latest=True,
            )
        )

        if lf_df.empty:
            df["lf"] = [0] * df.shape[0]
            return df

        # execlud lf column (will be calculated in later stage)
        lf_df = lf_df[
            [
                "dept_date",
                "cabin",
                "flt_num",
                "origin",
                "destination",
                "airline_code",
                "cap",
                "total_booking",
            ]
        ]

        lf_df = self.__get_lf_grouped_by_dept_date(lf_df)
        merged = df.merge(lf_df, left_on=["date"], right_on=["dept_date"], how="left")
        merged.dept_date.fillna("**", inplace=True)
        return merged

    def __get_lf_grouped_by_dept_date(self, data: pd.DataFrame) -> pd.DataFrame:
        lf_df = data[~(data.cap.isna()) & (~data.total_booking.isna())]
        if lf_df.empty:
            return pd.DataFrame(columns=["dept_date", "lf"])

        lf_df = (
            lf_df.groupby("dept_date")
            .apply(lambda _df: round((_df.total_booking.sum() / _df.cap.sum()) * 100))
            .reset_index()
            .rename(columns={0: "lf"})
        )
        return lf_df

    def __get_cabins(self, data: pd.DataFrame) -> List[str]:
        host_cabins = data[~data.cabin_host.isna()].cabin_host.unique().tolist()
        comp_cabins = data[~data.cabin_comp.isna()].cabin_comp.unique().tolist()
        cabins = list(map(CabinMapper.normalize, (set(host_cabins + comp_cabins))))
        return cabins

    def __get_date_range(self, data: pd.DataFrame) -> Tuple[int, int]:
        start_date = Date(data.date.min()).noramlize()
        end_date = Date(data.date.max()).noramlize()
        return start_date, end_date


@dataclass
class AttachAvgFare:
    event_data: pd.DataFrame
    form: EventCalendarForm
    host_code: str

    def get(self) -> pd.DataFrame:
        return self.event_data


@dataclass
class AttachPax:
    event_data: pd.DataFrame
    form: EventCalendarForm
    host_code: str

    def get(self) -> pd.DataFrame:
        return self.event_data


@dataclass
class AttachRask:
    event_form: pd.DataFrame
    form: EventCalendarForm
    host_code: str

    def get(self) -> pd.DataFrame:
        df = self.event_form.copy()
        return df


@dataclass
class AttachMinFare:
    events: pd.DataFrame
    form: EventCalendarForm
    host_code: str

    def get(self) -> pd.DataFrame:
        start_date = Date(self.events.s_date.min()).noramlize()
        end_date = Date(self.events.e_date.max()).noramlize()
        origin = self.form.get_origin()
        destination = self.form.get_destination()
        host, comp = CityBasedMarket(self.host_code, origin[0], destination[0]).competitors()[0:2]
        query = CalendarFareQuery(self.form, host, comp, (start_date, end_date)).query
        fares_df = pd.DataFrame(fare_repo.aggregate(query))

        if fares_df.empty:
            fares_df = pd.DataFrame(columns=["str_departure_date", "carrier_code", "currency"])

        comparison_fares_df = self.__build_comparison(fares_df, host, comp)
        merged = pd.merge(
            left=self.events,
            right=comparison_fares_df,
            left_on=["date"],
            right_on=["str_departure_date"],
            how="left",
        )

        if merged.empty:
            return pd.DataFrame(
                columns=[
                    "event_name",
                    "s_date",
                    "e_date",
                    "type",
                    "sub_type",
                    "city",
                    "countries",
                    "carrier_code_host",
                    "fare_host",
                    "currency_host",
                    "carrier_code_comp",
                    "fare_comp",
                    "currency_comp",
                    "date",
                    "cabin",
                ]
            )

        return merged

    def __build_comparison(self, fares_data: pd.DataFrame, host_code: str, comp_code: str) -> pd.DataFrame:
        mergeby = ["str_departure_date"]
        host = self.__carrier_fare_data(fares_data, host_code)
        comp = self.__carrier_fare_data(fares_data, comp_code)

        if host.empty:
            host = pd.DataFrame(
                columns=["carrier_code", "fare", "currency", "cabin", "str_departure_date", "cap", "total_booking"]
            )

        if comp.empty:
            comp = pd.DataFrame(
                columns=["carrier_code", "fare", "currency", "cabin", "str_departure_date", "cap", "total_booking"]
            )

        m = pd.merge(left=host, right=comp, left_on=mergeby, right_on=mergeby, how="outer", suffixes=("_host", "_comp"))
        return m

    def __carrier_fare_data(self, fares_data: pd.DataFrame, carrier_code: str) -> pd.DataFrame:
        df = fares_data[fares_data.carrier_code == carrier_code]

        if df.empty:
            return pd.DataFrame(columns=["str_departure_date", "fare", "currency", "carrier_code"])

        return df


class Field(Enum):
    PAX = "pax"
    AVG = "favg"
    LF = "lf"
    RASK = "rask"


class FieldMap(TypedDict):
    pax: Type[AttachPax]
    favg: Type[AttachAvgFare]
    lf: Type[AttachLoadFactor]
    rask: Type[AttachRask]


FIELD_MAP: FieldMap = {
    Field.AVG.value: AttachAvgFare,
    Field.PAX.value: AttachPax,
    Field.LF.value: AttachLoadFactor,
    Field.RASK.value: AttachRask,
}


@dataclass
class AttachValues:
    events: pd.DataFrame
    form: EventCalendarForm
    host_code: str

    def get(self) -> pd.DataFrame:
        Handler = FIELD_MAP[self.form.get_field()]
        df = self.events.copy()

        if self.form.should_consider_stats():
            df = AttachMinFare(df, self.form, self.host_code).get()
            df = Handler(df, self.form, self.host_code).get()
            df = df.replace("**", None)

        return df
