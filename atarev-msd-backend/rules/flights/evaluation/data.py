from dataclasses import dataclass
from datetime import datetime
from typing import List

import pandas as pd

from base.helpers.cabin import CabinMapper
from base.helpers.datetime import Date
from fares.common.query import LoadFactorQuery
from fares.repository import FareRepository
from flight_inventory.repository import FlightInventoryRepository
from rules.flights.evaluation.form import RuleEvaluationForm
from rules.flights.evaluation.query import FaresQuery, SchedQuery
from rules.flights.evaluation.types import Flight
from rules.repository import RuleRepository
from schedule.repository import ScheduleRepository

fare_repo = FareRepository()
sched_repo = ScheduleRepository()
rule_repo = RuleRepository()
inv_repo = FlightInventoryRepository()


@dataclass
class FareData:
    form: RuleEvaluationForm

    def get(self) -> pd.DataFrame:
        pipeline = FaresQuery(self.form).query
        df = pd.DataFrame(fare_repo.aggregate(pipeline))

        if df.empty:
            return pd.DataFrame(
                columns=[
                    "carrier_code",
                    "origin",
                    "destination",
                    "cabin",
                    "departure_date",
                    "departure_time",
                    "arrivale_date",
                    "arrivale_time",
                    "flt_num",
                    "fare",
                    "currency",
                    "is_connecting",
                    "op_code",
                    "mk_code",
                ]
            )

        df.cabin = df.cabin.str.upper()
        df = self.__attach_load_factor(df)
        return df

    def __attach_load_factor(self, data: pd.DataFrame) -> pd.DataFrame:
        lf_match = LoadFactorQuery(
            self.form.host_code,
            self.form.origin,
            self.form.destination,
            int(data.departure_date.min()),
            int(data.departure_date.max()),
        ).query

        lf_df = pd.DataFrame(inv_repo.get_load_factor(lf_match, [self.form.cabin]))

        if lf_df.empty:
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
                    "cabin_norm",
                    "date",
                    "dept_date",
                    "lf",
                    "dept_time",
                    "inserted_date",
                    "inserted_at",
                    "airline_code",
                    "market",
                ]
            )

        data["cabin_norm"] = data.cabin.apply(CabinMapper.normalize)
        lf_df["norm_dept_date"] = lf_df.dept_date.apply(lambda val: Date(val).noramlize())

        merged = pd.merge(
            left=data,
            right=lf_df,
            left_on=["origin", "destination", "departure_date", "flt_num", "cabin_norm"],
            right_on=["origin", "destination", "norm_dept_date", "flt_num", "cabin"],
            how="left",
        )

        merged = merged[
            [
                "carrier_code",
                "origin",
                "destination",
                "cabin_x",
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
                "cabin_norm",
                "date",
                "dept_date",
                "lf",
                "dept_time",
                "inserted_date",
                "inserted_at",
                "airline_code",
                "market",
            ]
        ].rename(columns={"cabin_x": "cabin"})
        merged.lf = merged.lf.fillna(-1)
        return merged


@dataclass
class ScheduleData:
    form: RuleEvaluationForm
    start_date: int
    end_date: int

    def get(self) -> List[Flight]:

        today = datetime.today().date()
        match = SchedQuery(self.form, self.start_date, self.end_date).query
        c = sched_repo.get(match=match, flatten=False)
        df = pd.DataFrame(c)

        if df.empty:
            return []

        df["dtd"] = df.str_departure_date.apply(lambda val: (datetime.strptime(val, "%Y-%m-%d").date() - today).days)
        df["dow"] = df.str_departure_date.apply(lambda val: datetime.strptime(val, "%Y-%m-%d").weekday())
        df["market"] = df.apply(lambda row: f"{row.origin}-{row.destination}", axis=1)
        return df.to_dict("records")


@dataclass
class RuleData:
    form: RuleEvaluationForm

    def get(self):
        return rule_repo.get({"carrier": self.form.host_code, "isActive": True, "type": {"$ne": "E"}})
