import os
from dataclasses import asdict, dataclass
from typing import Dict, List, Literal, Union

import pandas as pd
from core.checker import Check
from core.db import Collection
from core.logger import Logger
from core.stream import Stream
from pydantic import BaseModel, Field

from .source import LoadFactorSource

logger = Logger(f"{os.getenv('LOG_FOLDER')}/sv_load_factor.log")


class Cabin(BaseModel):
    code: Literal["Y", "J", "F"]
    load_factor: int
    total_booking: int = Field(init=False, default=0)
    total_group_booking: int = Field(init=False, default=0)
    overbooking_level: int = Field(init=False, default=0)
    capacity: int = Field(init=False, default=0)
    allotment: None = Field(init=False, default=None)
    available_seats: None = Field(init=False, default=None)


@dataclass
class Handler:
    date: int
    path: str

    def __post_init__(self):
        try:
            self.data = LoadFactorSource(self.date, self.path).data
            check = Check(self.data, "sv_load_factor.log")
            check.is_empty(f"lf.{self.date}.csv", " (SV Load Factor)")

        except FileNotFoundError:
            logger.error(f"file lf.{self.date}.csv was not found (SV Load Factor)")

    def parse(self):
        stream = Stream(Collection.INVENTORY)
        for (origin, destination, dept_date, flt_num), g_df in self.data.groupby(
            ["origin", "destination", "departure_date", "flt_num"]
        ):

            cabins = []

            for cabin, cabin_grouped in g_df.groupby("cabin"):
                data = cabin_grouped.iloc[0].to_dict()
                cabins.append(Cabin(code=cabin, load_factor=data["lf"]).model_dump())

            print(cabins)
            raise ValueError()

        #     data = g_df.iloc[0].to_dict()
        #     capacity_j = int(data["capacity_j"]) if type(data["capacity_j"]) is not str else data["capacity_j"]
        #     capacity_y = int(data["capacity_y"]) if type(data["capacity_y"]) is not str else data["capacity_y"]
        #     # cabinj = Cabin("J", data["booked_j"], data["booked_grp_j"], data["adj"], capacity_j)
        #     # cabiny = Cabin("Y", data["booked_y"], data["booked_grp_y"], data["adj"], capacity_y)
        #     cabinj = Cabin(
        #         code="J",
        #         total_booking=data["booked_j"],
        #         total_group_booking=data["booked_grp_j"],
        #         capacity=capacity_j,
        #     )
        #     cabiny = Cabin(
        #         code="Y",
        #         total_booking=data["booked_y"],
        #         total_group_booking=data["booked_grp_y"],
        #         capacity=capacity_y,
        #     )
        #     legs = [Leg(origin, destination, int(flt_num), int(dept_date), int(dept_time), [cabinj, cabiny])]
        #     segment = Segment("CY", origin, destination, int(flt_num), int(dept_date), int(dept_date), int(dept_time), legs)
        #     stream.update(
        #         {**asdict(segment), "date": int(self.date)},
        #         {
        #             "airline_code": "CY",
        #             "origin": origin,
        #             "destination": destination,
        #             "flight_number": int(flt_num),
        #             "departure_date": int(dept_date),
        #             "date": int(self.date),
        #         },
        #         upsert=True,
        #     )

        # stream.update(upsert=True)
        # logger.info("CY Load Factor data has been uploaded successfully !")
