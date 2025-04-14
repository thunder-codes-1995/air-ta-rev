import calendar
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Literal, Union

import pandas as pd

from base.helpers.user import User
from events.repository import EventRepository
from events.table.data import Data, EventData
from events.table.fields import PERIOD, PeriodSuffix
from events.table.form import EventTableForm
from events.table.handlers.event import Event
from events.table.handlers.hierarchy import Hierarchical, Level

event_repo = EventRepository()


@dataclass
class Summary:
    events_df: pd.DataFrame
    data: pd.DataFrame

    def __hanld_field(self, field_name: str, typ: Literal["event", "stats"]) -> Union[None, bool, int, float, str]:
        source = self.events_df if typ == "event" else self.data
        target = source[~source[field_name].isna()]

        if target.empty:
            return None

        values = target[field_name].unique().tolist()
        return values[0] if len(values) == 1 else None

    def __capacity(self, suffix: str) -> int:
        if self.data.empty:
            return 0
        return int(self.data[f"cap{suffix}"].fillna(0).sum())

    def get(self) -> Dict[str, Union[int, float, str]]:

        event_fields = [
            f"str_start_date{PeriodSuffix.FUTURE.value}",
            f"str_end_date{PeriodSuffix.FUTURE.value}",
            f"start_dow{PeriodSuffix.FUTURE.value}",
            f"end_dow{PeriodSuffix.FUTURE.value}",
            f"str_start_date{PeriodSuffix.PAST.value}",
            f"str_end_date{PeriodSuffix.PAST.value}",
            f"start_dow{PeriodSuffix.PAST.value}",
            f"end_dow{PeriodSuffix.PAST.value}",
            "city",
            "country_code",
            "type",
            "sub_type",
            "is_loc_fixed",
            "is_date_fixed",
        ]

        fields = ["market", "segment", "cabin", "dept_date"]

        event_data = {f: None for f in event_fields}
        data = {f: None for f in fields}

        if not self.events_df.empty:
            event_data = {f: self.__hanld_field(f, "event") for f in event_fields}

        if not self.data.empty:
            data = {f: self.__hanld_field(f, "stats") for f in fields}

        return {
            **event_data,
            **data,
            f"cap{PeriodSuffix.PAST.value}": self.__capacity(f"{PeriodSuffix.PAST.value}"),
            f"cap{PeriodSuffix.FUTURE.value}": self.__capacity(f"{PeriodSuffix.FUTURE.value}"),
        }


@dataclass
class TableItem:
    event_data: pd.DataFrame
    data: pd.DataFrame
    host_code: str
    date: str

    def get(self):
        res = []

        groupby0 = ["country_code"]
        groupby1 = ["country_code", "market"]
        groupby2 = ["country_code", "market", "cabin", "segment"]
        groupby3 = ["country_code", "market", "cabin", "segment", "dept_date", "flt_num"]
        level0 = Level(self.data, groupby0, 0, self.host_code, False).get()
        level1 = Level(self.data, groupby1, 1, self.host_code, False).get()
        level2 = Level(self.data, groupby2, 2, self.host_code, False).get()
        level3 = Level(self.data, groupby3, 3, self.host_code, True).get()
        countries = list(set(self.data.country_code.unique().tolist() + self.event_data.country_code.unique().tolist()))
        countries = list(filter(lambda c: c != "-", countries))
        events = Event(self.event_data).get()

        summary = Summary(events, level3).get()

        for country_code in countries:
            hierarchy_target = level0[level0.country_code == country_code]
            event = events[(events.country_code == country_code) & (events.date == self.date)]

            hierarchy = Hierarchical(
                hierarchy_target,
                level1,
                level2,
                level3,
                groupby0,
                groupby1,
                groupby2,
                groupby3,
                event,
            ).get()

            res += hierarchy

        return {
            "level": 0,
            "weekday": self.__weekday(),
            "lf": self.__load_factor(),
            "count": self.__count(),
            "date": self.date,
            **summary,
            "data": res,
        }

    def __load_factor(self) -> float:
        target = self.data[(self.data.carrier_code == self.host_code) & (self.data.flag == PERIOD.FUTURE.value)]
        target = target[~target.cap.isna()]

        if target.empty:
            return float(0)

        lf = lambda grouped_df: round((grouped_df.total_booking.sum() / grouped_df.cap.sum()) * 100)
        res = target.groupby(["dept_date"]).apply(lf)
        return float(res[self.date])

    def __count(self) -> int:
        return self.event_data[~self.event_data.event_name.isna()].shape[0]

    def __weekday(self) -> str:
        return calendar.day_abbr[datetime.strptime(self.date, "%Y-%m-%d").weekday()]


@dataclass
class EventTable:
    form: EventTableForm
    user: User

    def get(self):
        res = []
        events_df = EventData(self.form, self.user.carrier).get()
        start, end = events_df.start_date.min(), events_df.end_date.max()
        data = Data(form=self.form, host_code=self.user.carrier, date_range=(start, end)).get()

        for day_month, g_df in events_df.groupby(["start_day_month"]):
            curr_data = data[data.dept_day_month == day_month]
            res.append(
                TableItem(
                    event_data=g_df,
                    data=curr_data,
                    host_code=self.user.carrier,
                    date=f"{self.form.year.data}-{day_month}",
                ).get()
            )

        return {"data": res, "labels": self.form.labels()}
