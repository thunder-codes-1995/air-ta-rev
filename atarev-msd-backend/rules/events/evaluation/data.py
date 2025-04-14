from dataclasses import dataclass

import pandas as pd

from airports.entities import Country
from base.helpers.datetime import Date
from events.common import EventSetup
from events.repository import EventRepository
from flight_inventory.repository import FlightInventoryRepository
from rules.events.evaluation.query import EventQuery, InventoryQuery
from rules.repository import RuleRepository

event_repo = EventRepository()
inventory_repo = FlightInventoryRepository()
rule_repo = RuleRepository()

bins = (365, 270, 180, 90, 30, 15, 7, 6, 5, 4, 3, 2, 1)


@dataclass
class RuleData:
    host_code: str

    def get(self):
        c = rule_repo.get({"carrier": self.host_code, "type": "E"})
        return c


@dataclass
class InventoryData:
    host_code: str
    departure_date: int

    def get(self) -> pd.DataFrame:
        match = InventoryQuery(self.host_code, self.departure_date).query
        inv = pd.DataFrame(inventory_repo.get_load_factor(extended=True, match=match))

        if inv.empty:
            return pd.DataFrame(columns=("country_code", "cabin", "date", "dept_date", "lf", "dte"))

        inv = self.__attach_country_code(inv)
        inv = self.__aggregate(inv)
        inv = self.__attach_dte(inv)
        return inv.sort_values("dte", ascending=False)

    def __attach_country_code(self, inv_df: pd.DataFrame) -> pd.DataFrame:
        m = Country.airport_country_map(inv_df.origin.unique().tolist())
        inv_df["country_code"] = inv_df.origin.map(m)
        return inv_df

    def __aggregate(self, inv_df: pd.DataFrame) -> pd.DataFrame:
        grouped_df = (
            inv_df.groupby(["country_code", "cabin", "date", "dept_date"])
            .apply(lambda grouped: round((grouped.total_booking.sum() / grouped.cap.sum()) * 100))
            .reset_index()
            .rename(columns={0: "lf"})
        )

        return grouped_df

    def __attach_dte(self, inv_df: pd.DataFrame) -> pd.DataFrame:
        inv_df["dte"] = inv_df.apply(lambda row: (Date(row.dept_date).date() - Date(row.date).date()).days, axis=1)
        return inv_df


@dataclass
class EventData:
    host_code: str

    def get(self) -> pd.DataFrame:
        match = EventQuery(self.host_code).query
        c = event_repo.get_events(match)
        df = pd.DataFrame(c)

        if df.empty:
            return pd.DataFrame(
                columns=[
                    "country_code",
                    "start_date",
                    "end_date",
                    "event_name",
                    "city",
                    "id",
                    "str_start_date",
                    "str_end_date",
                    "type",
                    "sub_type",
                ]
            )

        df = EventSetup.flatten(df, "type", "sub_type")
        return df.sort_values("start_date")
