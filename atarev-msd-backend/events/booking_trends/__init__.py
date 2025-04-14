from dataclasses import dataclass
from datetime import datetime
from typing import List, TypedDict

import pandas as pd

from base.middlewares import attach_figure_id
from events.booking_trends.data import PaxData
from events.booking_trends.figure import PaxBookingTrendsFigure
from events.booking_trends.form import BookingTrendsForm
from utils.funcs import table_to_text


class Event(TypedDict):
    event_name: str
    str_start_date: str
    str_end_date: str
    dow: str
    event_year: int
    event_month: int
    event_idx: str
    days_count: int


@dataclass
class PaxBookingTrends:

    form: BookingTrendsForm
    host_code: str

    def __events(self, all_events: pd.DataFrame) -> List[Event]:
        events = all_events[~all_events.event_name.isna()]
        if events.empty:
            return []

        return (
            events[
                [
                    "country_code",
                    "event_name",
                    "str_start_date",
                    "str_end_date",
                    "dow",
                    "event_year",
                    "event_month",
                    "event_idx",
                    "days_count",
                ]
            ].to_dict("records"),
        )

    @attach_figure_id(["fig"])
    def get(self):
        data = PaxData(form=self.form, host_code=self.host_code).get()
        figure = PaxBookingTrendsFigure(pax_data=data, form=self.form, host_code=self.host_code).get()

        return {
            "fig": figure,
            "today_date": datetime.now().strftime("%Y-%m-%d"),
            "story_text": {
                "main_card": {
                    "content": table_to_text(data, [("pax", "sum")]) if not data.empty else "",
                    "title": "Graph Text",
                }
            },
            "holidays_table": {
                "data ": self.__events(data),
                "labels": [
                    {"event_name": "Name"},
                    {"str_start_date": "Start Date"},
                    {"str_end_date": "End Date"},
                    {"country_code": "Country"},
                    {"dow": "Day-of-Week"},
                    {"days_count": "Length"},
                ],
            },
        }
