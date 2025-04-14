from dataclasses import dataclass

import pandas as pd
from core.checker import Check
from core.meta import read_meta

from .meta import FlightLegInventoryMeta, LegClassRESMeta, SegmentClassRESMeta


@dataclass
class SegmentSource:
    date: int
    path: str

    def __post_init__(self):
        data = read_meta(SegmentClassRESMeta, self.date, self.path, True)
        self.__check(data)
        self.data = data.fillna("-")

    def __check(self, data: pd.DataFrame):
        Check(data, "hitit_py_daily_csv.log").is_empty(f"{SegmentClassRESMeta.name}.{self.date}.csv", " (Hitit-PY-Daily CSV)")


@dataclass
class LegSource:
    date: int
    path: str

    def __post_init__(self):
        leg_cls_res_data = read_meta(LegClassRESMeta, self.date, self.path, True)
        flt_leg_inv_data = read_meta(FlightLegInventoryMeta, self.date, self.path, True)

        self.__check(leg_cls_res_data, flt_leg_inv_data)

        leg_cls_res_data.fillna("-", inplace=True)
        flt_leg_inv_data.fillna("-", inplace=True)

        self.data = leg_cls_res_data.merge(
            flt_leg_inv_data,
            on=[
                "airline_code",
                "flt_number",
                "flt_number_ext",
                "flt_dept_date",
                "leg_dept_date",
                "cabin",
                "leg_origin",
            ],
            suffixes=("_left", "_right"),
        )

    def __check(self, leg_cls_res: pd.DataFrame, flt_leg_inv: pd.DataFrame):
        log_path = "hitit_py_daily_csv.log"
        Check(leg_cls_res, log_path).is_empty(f"{LegClassRESMeta.name}.{self.date}.csv", " (Hitit-PY-Daily CSV)")
        Check(flt_leg_inv, log_path).is_empty(f"{FlightLegInventoryMeta.name}.{self.date}.csv", " (Hitit-PY-Daily CSV)")
