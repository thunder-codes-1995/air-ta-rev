import os
from dataclasses import asdict, dataclass, field
from typing import List

from handlers.cyp.load_factor.serializer import CabinSerializer, LegSerializer, SegmentSerializer
from handlers.cyp.load_factor.source import LoadFactorSource
from share.core.checker import Check
from share.core.db import Collection
from share.core.logger import Logger
from share.core.stream import Stream

logger = Logger(f"{os.getenv('LOG_FOLDER')}/cy_load_factor.log")


@dataclass
class Cabin:
    code: str
    total_booking: int
    total_group_booking: int
    # overbooking_level: int
    capacity: int = field(default=None)
    allotment: int = field(init=False, default=None)
    available_seats: int = field(init=False, default=None)
    load_factor: int = field(init=False, default=None)

    def __post_init__(self):
        serializer = CabinSerializer(
            data={
                "code": self.code,
                "total_booking": self.total_booking,
                "total_group_booking": self.total_group_booking,
                # "overbooking_level": self.overbooking_level,
                "capacity": self.capacity,
                "allotment": self.allotment,
                "available_seats": self.available_seats,
            }
        )

        serializer.is_valid()
        self.load_factor = serializer.data["load_factor"]

    def classes(self):
        return []


@dataclass
class Leg:
    origin: str
    destination: str
    flight_number: int
    departure_date: int
    departure_time: int
    cabins: List[Cabin] = field(default_factory=list)
    flight_number_ext: str = field(init=False, default="-")
    load_factor: int = field(init=False, default=None)
    arrival_date: int = field(init=False, default=None)
    arrival_time: int = field(init=False, default=None)

    def __post_init__(self):
        LegSerializer(
            data={
                "origin": self.origin,
                "destination": self.destination,
                "flight_number": self.flight_number,
                "departure_date": self.departure_date,
                "departure_time": self.departure_time,
                "age": 33,
            }
        ).is_valid()
        self.__load_factor()

    def __load_factor(self) -> None:
        sum_booked = 0
        sum_capacity = 0

        for cabin in self.cabins:
            if type(cabin.capacity) is int:
                sum_capacity += cabin.capacity
            if type(cabin.total_booking) is int:
                sum_booked += cabin.total_booking

        if sum_capacity == 0:
            return

        self.load_factor = round((sum_booked / sum_capacity) * 100)


@dataclass
class Segment:
    airline_code: str
    origin: str
    destination: str
    flight_number: int
    departure_date: int
    flight_departure_date: int
    departure_time: int
    flight_number_ext: str = field(init=False, default="-")
    legs: List[Leg] = field(default_factory=list)

    def __post_init__(self):
        SegmentSerializer(
            data={
                "carrier_code": self.airline_code,
                "origin": self.origin,
                "destination": self.destination,
                "flight_number": self.flight_number,
                "departure_date": self.departure_date,
                "departure_time": self.departure_time,
            }
        ).is_valid()


@dataclass
class Handler:
    date: int
    path: str

    def __post_init__(self):
        try:
            self.data = LoadFactorSource(self.date, self.path).data
            check = Check(self.data, "cy_load_factor.log")
            check.is_empty(f"lf.{self.date}.csv", " (CY Load Factor)")

        except FileNotFoundError:
            logger.error(f"file lf.{self.date}.csv was not found (CY Load Factor)")

    def parse(self):
        stream = Stream(Collection.INVENTORY)
        for (origin, destination, dept_date, dept_time, flt_num), g_df in self.data.groupby(
            ["origin", "destination", "norm_dept_time", "dept_time", "flt_num"]
        ):
            data = g_df.iloc[0].to_dict()
            capacity_j = int(data["capacity_j"]) if type(data["capacity_j"]) is not str else data["capacity_j"]
            capacity_y = int(data["capacity_y"]) if type(data["capacity_y"]) is not str else data["capacity_y"]
            # cabinj = Cabin("J", data["booked_j"], data["booked_grp_j"], data["adj"], capacity_j)
            # cabiny = Cabin("Y", data["booked_y"], data["booked_grp_y"], data["adj"], capacity_y)
            cabinj = Cabin(
                code="J",
                total_booking=data["booked_j"],
                total_group_booking=data["booked_grp_j"],
                capacity=capacity_j,
            )
            cabiny = Cabin(
                code="Y",
                total_booking=data["booked_y"],
                total_group_booking=data["booked_grp_y"],
                capacity=capacity_y,
            )
            legs = [Leg(origin, destination, int(flt_num), int(dept_date), int(dept_time), [cabinj, cabiny])]
            segment = Segment("CY", origin, destination, int(flt_num), int(dept_date), int(dept_date), int(dept_time), legs)
            stream.update(
                {**asdict(segment), "date": int(self.date)},
                {
                    "airline_code": "CY",
                    "origin": origin,
                    "destination": destination,
                    "flight_number": int(flt_num),
                    "departure_date": int(dept_date),
                    "date": int(self.date),
                },
                upsert=True,
            )

        stream.update(upsert=True)
        logger.info("CY Load Factor data has been uploaded successfully !")
