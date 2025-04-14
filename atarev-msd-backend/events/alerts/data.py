from dataclasses import dataclass
from typing import List, TypedDict, cast

import pandas as pd

from base.helpers.cabin import CabinMapper
from base.helpers.datetime import Date, Time
from rules.repository import RuleResultRepository

rule_res_repo = RuleResultRepository()


class Alert(TypedDict):
    created_at: str
    carrier: str
    rule_name: str
    event_name: str
    country_code: str
    type: str
    sub_type: str
    start_date: str
    end_date: str
    cabin: str
    lf_pickup_ratio: int
    lf_pickup_value: int
    lf_pickup_range: str


@dataclass
class AlertData:
    host_code: str

    def get(self) -> List[Alert]:
        c = rule_res_repo.get(flatten=True)
        df = pd.DataFrame(c)
        df.cabin = df.cabin.apply(CabinMapper.humanize)
        df.created_at = df.created_at.apply(self.__humanize_created_at)
        df = df[
            [
                "created_at",
                "carrier",
                "rule_name",
                "event_name",
                "country_code",
                "type",
                "sub_type",
                "str_start_date",
                "str_end_date",
                "cabin",
                "lf_pickup_ratio",
                "lf_pickup_value",
                "lf_pickup_range",
            ]
        ]
        df = df.rename(columns={"str_start_date": "start_date", "str_end_date": "end_date"})
        res = cast(List[Alert], df.to_dict("records"))
        return res

    def __humanize_created_at(self, value: int) -> str:
        as_string = f"{value}"
        dt = Date(int(as_string[0:8])).humanize()
        time = Time(int(as_string[8:12])).humanize()[0:5]
        return f"{dt} {time}"
