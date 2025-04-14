from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Tuple

import pandas as pd

from airports.repository import AirportRepository
from base.entities.carrier import Carrier
from base.helpers.datetime import Date
from base.mongo_utils import convert_list_param_to_criteria, merge_criterions
from rules.events.evaluation.form import AlertEvaluationForm

airport_repo = AirportRepository()


@dataclass
class EventQuery:
    host_code: str

    @property
    def query(self):
        dt_range = self.__date_range()
        return merge_criterions(
            [
                convert_list_param_to_criteria("airline_code", self.host_code),
                convert_list_param_to_criteria("country_code", self.__countries()),
                {"$and": [{"start_date": {"$gte": dt_range[0]}}, {"start_date": {"$lte": dt_range[1]}}]},
            ]
        )

    def __countries(self) -> List[str]:
        c = Carrier(self.host_code).city_based_markets()
        market_df = pd.DataFrame(c)
        cities = list(set(market_df.orig.unique().tolist() + market_df.dest.unique().tolist()))
        countries = list(airport_repo.get_countries_for_cities(cities))
        return countries

    def __date_range(self) -> Tuple[int, int]:
        today = datetime.today()
        one_year_in_future = today + timedelta(days=365)
        return Date(today).noramlize(), Date(one_year_in_future).noramlize()


@dataclass
class InventoryQuery:
    host_code: str
    departure_date: int

    @property
    def query(self):

        return {
            "airline_code": self.host_code,
            "departure_date": self.departure_date,
            "date": {"$in": self.__dates_from_bins()},
        }

    def __dates_from_bins(self) -> List[int]:
        bins = (365, 270, 180, 90, 30, 15, 7, 6, 5, 4, 3, 2, 1)
        dept_dt = Date(self.departure_date).date()
        return [Date(dept_dt - timedelta(days=bin)).noramlize() for bin in bins]


@dataclass
class RemoveOldAlertsQuery:
    form: AlertEvaluationForm

    @property
    def query(self):
        return merge_criterions(
            [
                convert_list_param_to_criteria("carrier", self.form.host_code),
                convert_list_param_to_criteria("type", "E"),
            ]
        )
