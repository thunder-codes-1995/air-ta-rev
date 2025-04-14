from dataclasses import asdict, dataclass, field
from pprint import pprint
from typing import Iterable, List

import numpy as np
import pandas as pd
from core.db import Collection
from core.stream import Stream

from .source import LegSource, SegmentSource


def dict_factor(obj, exclude_fields: Iterable[str]) -> dict:
    return {k: v for (k, v) in obj if ((v is not None) and (k not in exclude_fields))}


@dataclass
class Class:
    code: str
    seats_available: int
    total_booking: int
    total_group_booking: int
    parent_booking_class: str
    authorization: int
    display_sequence: int


@dataclass
class Cabin:
    code: str
    capacity: int
    allotment: int
    total_booking: int
    total_group_booking: int
    available_seats: int
    overbooking_level: int
    cabin_df: pd.DataFrame = field(repr=False)
    classes: List[Class] = field(init=False, default=None, repr=False)
    load_factor: int = field(init=False, default=None)

    @property
    def lf(self) -> int:
        if type(self.capacity) is str or not self.total_booking:
            return
        return round(self.total_booking / self.capacity * 100)

    def __post_init__(self):
        class_grouped_df = self.cabin_df.groupby("class", as_index=False).aggregate(
            {
                "class_seats_avail": "sum",
                "total_booking": "sum",
                "total_grp_booking": "sum",
            }
        )

        merged = class_grouped_df.merge(
            self.cabin_df[
                [
                    "class",
                    "class_authorization",
                    "parent_booking_class",
                    "display_sequence",
                ]
            ].drop_duplicates("class"),
            on=["class"],
        )

        merged.sort_values("display_sequence", inplace=True)

        self.load_factor = self.lf
        self.classes = [
            Class(
                **{
                    **obj,
                    "seats_available": int(obj["seats_available"]),
                    "total_booking": int(obj["total_booking"]),
                    "total_group_booking": int(obj["total_group_booking"]),
                    "authorization": int(obj["authorization"]),
                    "display_sequence": int(obj["display_sequence"]),
                }
            )
            for obj in merged[
                [
                    "class",
                    "class_seats_avail",
                    "total_booking",
                    "total_grp_booking",
                    "parent_booking_class",
                    "class_authorization",
                    "display_sequence",
                ]
            ]
            .rename(
                columns={
                    "class": "code",
                    "class_seats_avail": "seats_available",
                    "total_grp_booking": "total_group_booking",
                    "class_authorization": "authorization",
                }
            )
            .to_dict("records")
        ]


@dataclass
class Leg:
    origin: str
    destination: str
    flight_number: int
    flight_number_ext: str
    departure_date: int
    arrival_date: int
    departure_time: int
    arrival_time: int
    leg_df: pd.DataFrame = field(repr=False)
    cabins: List[Cabin] = field(init=False, default=None, repr=False)

    def __post_init__(self):
        result = []

        for cabin, cabin_df in self.leg_df.groupby("cabin"):
            (
                cabin,
                capacity,
                total_grp_booking,
                total_booking,
                allotment,
                overbooking,
                avail_seats,
            ) = cabin_df[
                [
                    "cabin",
                    "capacity",
                    "total_grp_booking_cabin",
                    "total_booking_cabin",
                    "cabin_allotment",
                    "cabin_level_over_booking",
                    "seats_available_in_cabin",
                ]
            ].iloc[0]

            result.append(
                Cabin(
                    code=cabin,
                    capacity=int(capacity),
                    total_group_booking=int(total_grp_booking),
                    total_booking=int(total_booking),
                    allotment=int(allotment),
                    overbooking_level=int(overbooking),
                    available_seats=int(avail_seats),
                    cabin_df=cabin_df,
                )
            )

        self.cabins = result


@dataclass
class Segment:
    airline_code: str
    origin: str
    destination: str
    flight_number: str
    flight_number_ext: str
    flight_departure_date: int
    departure_date: int
    legs_df: pd.DataFrame = field(repr=False)
    legs: List[Leg] = field(init=False, default=None, repr=False)

    def __post_init__(self):
        result = []
        segment_legs_df = self.__get_legs_df()
        legs_ordered = list(zip(segment_legs_df.leg_origin.unique(), segment_legs_df.leg_destination.unique()))

        for leg_origin, leg_destination in legs_ordered:
            leg_df = segment_legs_df[
                (segment_legs_df.leg_origin == leg_origin) & (segment_legs_df.leg_destination == leg_destination)
            ]
            (
                flt_number,
                flt_number_ext,
                leg_dept_date,
                leg_arrival_date,
                leg_arrival_time,
                leg_dept_time,
            ) = leg_df[
                [
                    "flt_number",
                    "flt_number_ext",
                    "leg_dept_date",
                    "leg_arrival_date",
                    "leg_arrival_time",
                    "leg_dept_time",
                ]
            ].iloc[0]

            result.append(
                Leg(
                    origin=leg_origin,
                    destination=leg_destination,
                    flight_number=str(flt_number),
                    flight_number_ext=flt_number_ext,
                    departure_date=int(leg_dept_date),
                    arrival_date=int(leg_arrival_date),
                    departure_time=int(leg_dept_time),
                    arrival_time=int(leg_arrival_time),
                    leg_df=leg_df,
                )
            )

        self.legs = result

    def __get_legs_df(self) -> pd.DataFrame:
        flt_legs_df = self.legs_df.sort_values("leg_index").reset_index()
        start_idx = (flt_legs_df.leg_origin == self.origin).idxmax()
        end_idx = flt_legs_df[flt_legs_df.leg_destination == self.destination].last_valid_index() + 1
        return flt_legs_df.iloc[start_idx:end_idx]


@dataclass
class Flight:
    airline_code: str
    flight_number: int
    flt_number_ext: str
    flt_dept_date: int
    segments_df: pd.DataFrame = field(repr=False)
    legs_data: pd.DataFrame = field(repr=False)
    segments: List[Segment] = field(init=False, default=None, repr=False)

    def __post_init__(self):
        result = []

        for _, (seg_origin, seg_dest, dept_date) in self.segments_df[
            [
                "seg_origin",
                "seg_destination",
                "seg_dept_date",
            ]
        ].iterrows():
            segment_legs = self.legs_data[
                (self.legs_data.flt_number == self.flight_number) & (self.legs_data.flt_dept_date == self.flt_dept_date)
            ]

            result.append(
                Segment(
                    self.airline_code,
                    seg_origin,
                    seg_dest,
                    int(self.flight_number),
                    self.flt_number_ext,
                    int(self.flt_dept_date),
                    int(dept_date),
                    segment_legs,
                )
            )
        self.segments = result


@dataclass
class Handler:
    date: int
    path: str

    def parse(self):
        stream = Stream(Collection.INVENTORY)
        segments_data = SegmentSource(self.date, self.path).data
        legs_data = LegSource(self.date, self.path).data

        for flt_g, flt_df in segments_data.groupby(["airline_code", "flt_number", "flt_number_ext", "flt_dept_date"]):
            al_code, flt_number, flt_number_ext, flt_dept_date = flt_g
            seg_df = flt_df.drop_duplicates(["seg_origin", "seg_destination"])
            flight = Flight(al_code, flt_number, flt_number_ext, flt_dept_date, seg_df, legs_data)

            for segment in flight.segments:
                obj = asdict(
                    segment,
                    dict_factory=lambda obj: dict_factor(obj, ("legs_data", "segments_df", "legs_df", "leg_df", "cabin_df")),
                )

                stream.update(
                    {**obj, "date": int(self.date)},
                    {
                        "airline_code": "PY",
                        "origin": segment.origin,
                        "destination": segment.destination,
                        "flight_number": int(segment.flight_number),
                        "departure_date": int(segment.departure_date),
                        "date": int(self.date),
                    },
                    upsert=True,
                )
