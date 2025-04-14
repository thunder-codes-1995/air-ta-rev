from dataclasses import dataclass
from typing import Dict, List, Tuple

import pandas as pd
from daily.source import LegSource, SegmentSource


@dataclass
class Flight:
    airline_code: str
    flight_number: int
    flight_ext: str
    dept_date: int
    data: pd.DataFrame
    leg_data: pd.DataFrame

    def get_segments(self) -> pd.DataFrame:
        """get unique segments dataframe"""
        unique_segments_df = self.data.drop_duplicates(["seg_origin", "seg_destination"])
        return unique_segments_df

    def parse(self):
        unique_segments_df = self.get_segments()
        cols: List[str] = ["seg_origin", "seg_destination", "seg_dept_date"]
        for row in unique_segments_df[cols].itertuples():
            # for each segment get legs data
            legs = Segment(row.seg_origin, row.seg_destination, row.seg_dept_date).get_legs(self.leg_data)
            for (origin, destination), market_df in legs.groupby(["leg_origin", "leg_destination"]):
                Leg(origin, destination, market_df).parse()

            # legs_ordered = list(zip(legs.leg_origin.unique(), legs.leg_destination.unique()))

            # for leg_origin, leg_destination in legs_ordered:
            #     leg_df = self.leg_data[
            #         (self.leg_data.leg_origin == leg_origin) & (self.leg_data.leg_destination == leg_destination)
            #     ]
            #     print(leg_df)
            #     Leg(leg_origin, leg_destination, leg_df).parse()


@dataclass
class Segment:
    origin: str
    destination: int
    dept_date: int

    def get_legs(self, flight_legs: pd.DataFrame) -> pd.DataFrame:
        """
        get legs that belong to a leg
        this method takes in legs data that belong to a flight and returns legs data that belong to selected flight
        """
        start_idx, end_idx = self.get_segment_slice(flight_legs)
        return flight_legs.iloc[start_idx:end_idx]

    def get_segment_slice(self, flight_legs: pd.DataFrame) -> Tuple[int, int]:
        """
        get segment chunk (segment start and end row indexes)
        origin of result should be origin of first leg and destiination should be destionation of last leg
        """
        start_idx = (flight_legs.leg_origin == self.origin).idxmax()
        end_idx = flight_legs[flight_legs.leg_destination == self.destination].last_valid_index() + 1
        return start_idx, end_idx


@dataclass
class Leg:
    origin: str
    destination: str
    data: pd.DataFrame

    def parse(self):
        for cabin, cabin_df in self.data.groupby("cabin"):
            Cls(cabin)


@dataclass
class Cls:
    cabin: str

    def parse(self):
        ...


@dataclass
class Daily:
    date: int
    path: str = "share/handlers/hitit/samples"

    def __post_init__(self):
        self.segments_source = SegmentSource(self.date, self.path).data
        self.legs_source = LegSource(self.date, self.path).data

    def parse(self):
        flight_columns: List[str] = [
            "airline_code",
            "flt_number",
            "flt_number_ext",
            "flt_dept_date",
        ]

        for (al_code, flt_num, flt_num_ext, flt_dept_date), flt_df in self.segments_source.groupby(flight_columns):
            # get all legs that belong to a flight (a flight has many segments and each segment has many legs)
            flight_legs = (
                self.legs_source[(self.legs_source.flt_number == flt_num) & (self.legs_source.flt_dept_date == flt_dept_date)]
                .sort_values("leg_index")
                .reset_index()
            )

            flight = Flight(al_code, flt_num, flt_num_ext, flt_dept_date, flt_df, flight_legs)
            flight.parse()


if __name__ == "__main__":
    # seg_df = SegmentSource(20221221, "share/handlers/hitit/samples").data
    # leg_df = LegSource(20221221, "share/handlers/hitit/samples").data

    Daily(20221221).parse()
