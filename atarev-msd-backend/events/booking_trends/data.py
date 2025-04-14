from dataclasses import dataclass
from datetime import date, datetime

import pandas as pd

from dds.repository import DdsRepository
from events.booking_trends.form import AggType, BookingTrendsForm
from events.booking_trends.query import BookingTrendsQuery, EventQuery
from events.repository import EventRepository

repo = DdsRepository()
event_repo = EventRepository()


@dataclass
class EventData:

    form: BookingTrendsForm
    host_code: str

    def get(self) -> pd.DataFrame:
        cols = [
            "country_code",
            "start_date",
            "end_date",
            "event_name",
            "str_start_date",
            "str_end_date",
            "formatted_end_date",
            "dow",
            "event_year",
            "event_month",
            "event_idx",
            "days_count",
        ]

        if not self.form.show_holidays():
            return pd.DataFrame(columns=cols)

        match = EventQuery(self.form, self.host_code).query
        df = pd.DataFrame(event_repo.get_events(match))

        if df.empty:
            return pd.DataFrame(columns=cols)

        df["end_dept_date"] = df.str_end_date.apply(lambda val: datetime.strptime(val, "%Y-%m-%d").date())
        df["start_dept_date"] = df.str_start_date.apply(lambda val: datetime.strptime(val, "%Y-%m-%d").date())
        df["formatted_end_date"] = df.end_dept_date.apply(lambda val: val.strftime("%d%b%y").upper())
        df["event_year"] = df.end_dept_date.apply(lambda val: val.year)
        df["event_month"] = df.end_dept_date.apply(lambda val: val.month)
        df["days_count"] = df.apply(lambda row: (row.end_dept_date - row.start_dept_date).days or 1, axis=1)

        return df[cols]


@dataclass
class PaxData:
    form: BookingTrendsForm
    host_code: str

    def get(self) -> pd.DataFrame:
        query = BookingTrendsQuery(self.form, self.host_code).query
        data = pd.DataFrame(repo.aggregate(query))
        events_data = EventData(self.form, self.host_code).get()

        if data.empty:
            return pd.DataFrame(
                columns=[
                    "event_name",
                    "country_code",
                    "pax",
                    "carrier_code",
                    "travel_month",
                    "travel_year",
                    "dept_date",
                    "str_travel_date",
                    "days_count",
                ]
            )

        if self.form.get_agg_type() == AggType.MONTHLY.value:
            data["dept_date"] = data.apply(lambda row: date(year=row.travel_year, month=row.travel_month, day=1), axis=1)
            data["str_travel_date"] = data.dept_date.apply(lambda val: val.strftime("%Y-%m-%d"))
        else:
            data["dept_date"] = data.str_travel_date.apply(lambda val: datetime.strptime(val, "%Y-%m-%d").date())

        data["formatted_dept_date"] = data.dept_date.apply(lambda val: val.strftime("%d%b%y").upper())
        data = data.sort_values("dept_date")
        merged = pd.merge(left=data, right=events_data, left_on=["str_travel_date"], right_on=["str_start_date"], how="left")
        return merged
