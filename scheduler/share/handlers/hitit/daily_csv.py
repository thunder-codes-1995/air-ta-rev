from pathlib import Path
from typing import List, Tuple

import pandas as pd
from core.env import HITIT_FILES_PATH
from core.logger import Logger
from models.schedule import Cabin, Class, Leg, _Segment


class CsvHandler:
    @property
    def seg_cls_cols(self) -> List[str]:
        return [
            "airline_code",
            "flt_number",
            "flt_number_ext",
            "flt_dept_date",
            "seg_dept_date",
            "seg_origin",
            "seg_destination",
            "class",
            "cabin",
            "total_booking_class",
            "total_grp_booking_class",
            "parent_booking_class",
            "class_seats_avail",
            "class_authorizations",
            "segment_closed_indicator",
            "display_sequence",
            "market_ignore",
            "wait_list",
            "seats_sold_limit",
        ]

    @property
    def leg_cls_cols(self) -> List[str]:
        return [
            "airline_code",
            "flt_number",
            "flt_number_ext",
            "flt_dept_date",
            "leg_dept_date",
            "leg_origin",
            "class",
            "cabin",
            "parent_booking_class",
            "total_booking",
            "total_grp_booking",
            "class_authorization",
            "class_seats_avail",
            "wait_count",
            "display_sequence",
        ]

    @property
    def flt_leg_cols(self) -> List[str]:
        return [
            "airline_code",
            "flt_number",
            "flt_number_ext",
            "leg_dept_date",
            "leg_origin",
            "leg_destination",
            "cabin",
            "cabin_capacity",
            "leg_index",
            "flt_dept_date",
            "leg_arrival_date",
            "leg_arrival_time",
            "leg_dept_time",
            "capacity",
            "total_grp_booking_cabin",
            "total_booking_cabin",
            "cabin_allotment",
            "wait_count",
            "cabin_level_over_booking",
            "seats_available_in_cabin",
            "aircraft_type",
        ]

    @property
    def seg_cls_path(self) -> str:
        """path to read SEGCLSRES files"""
        return f"{self.path_to_files}/SEGCLSRES.{self.date}"

    @property
    def leg_cls_path(self) -> str:
        """path to read LEGCLSRES files"""
        return f"{self.path_to_files}/LEGCLSRES.{self.date}"

    @property
    def flt_leg_path(self) -> str:
        """path to read FLTLEGINV files"""
        return f"{self.path_to_files}/FLTLEGINV.{self.date}"

    def __handle_segments(
        self,
        al_code: str,
        flt_number: int,
        flt_number_ext: str,
        flt_dept_date: int,
        seg_df: pd.DataFrame,
    ) -> List[_Segment]:
        """get all segments belong to one flight"""

        segments = []
        for _, seg_origin, seg_dest, dept_date in seg_df[
            ["seg_origin", "seg_destination", "seg_dept_date"]
        ].itertuples():
            # fmt: off
            legs_df = self.get_segment_legs(seg_origin,seg_dest, flt_number, flt_dept_date)
            segments.append(_Segment(
                origin =seg_origin,
                destination=seg_dest,
                airline_code =al_code,
                flight_number=str(flt_number),
                flight_number_ext=flt_number_ext,
                flight_departure_date=int(flt_dept_date),
                departure_date=int(dept_date),
                legs = self.__handle_legs(legs_df)
            ))

        return segments

    def __handle_legs(self, legs_df: pd.DataFrame) -> List[Leg]:
        """get all legs belong to one segment"""
        legs = []
        legs_ordered = list(
            zip(legs_df.leg_origin.unique(), legs_df.leg_destination.unique())
        )
        for leg_origin, leg_destination in legs_ordered:
            leg_df = legs_df[
                (legs_df.leg_origin == leg_origin)
                & (legs_df.leg_destination == leg_destination)
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
            ].iloc[
                0
            ]

            legs.append(
                Leg(
                    origin=leg_origin,
                    destination=leg_destination,
                    flight_number=str(flt_number),
                    flight_number_ext=flt_number_ext,
                    departure_date=int(leg_dept_date),
                    arrival_date=int(leg_arrival_date),
                    departure_time=int(leg_dept_time),
                    arrival_time=int(leg_arrival_time),
                    cabins=self.__handle_cabins(leg_df),
                )
            )
        return legs

    def __handle_cabins(self, leg_df: pd.DataFrame) -> List[Cabin]:
        """get all cabins belong to same leg"""
        cabins = []
        x = leg_df[
            (leg_df.leg_origin == "PBM")
            & (leg_df.leg_destination == "AUA")
            & (leg_df.flt_number == 631)
            & (leg_df.flt_dept_date == 20221222)
            & (leg_df.leg_dept_date == 20221222)
        ]
        for cabin, cabin_df in leg_df.groupby("cabin"):
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
            ].iloc[
                0
            ]

            cabins.append(
                Cabin(
                    code=cabin,
                    capacity=int(capacity),
                    total_group_booking=int(total_grp_booking),
                    total_booking=int(total_booking),
                    allotment=int(allotment),
                    overbooking_level=int(overbooking),
                    available_seats=int(avail_seats),
                    classes=self.__handle_classes(cabin_df),
                )
            )
        return cabins

    def __handle_classes(self, cabin_df: pd.DataFrame) -> List[Class]:
        """get all classes belong to one cabin"""
        class_grouped_df = cabin_df.groupby("class", as_index=False).aggregate(
            {
                "class_seats_avail": "sum",
                "total_booking": "sum",
                "total_grp_booking": "sum",
            }
        )

        merged = class_grouped_df.merge(
            cabin_df[
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

        return [
            Class(**obj)
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

    def get_segment_legs(
        self, seg_origin: str, seg_destination: str, flt_number: int, flt_dept_date: int
    ) -> pd.DataFrame:
        """get all possible (ordered) legs for one segments"""
        # fmt: off
        flt_legs_df = self.legs[(self.legs.flt_number == flt_number) & (self.legs.flt_dept_date == flt_dept_date)].sort_values("leg_index")
        flt_legs_df = flt_legs_df.reset_index()
        start_idx,end_idx = self.get_segment_slice(flt_legs_df,seg_origin,seg_destination)
        # get legs belong to specified segment 
        return flt_legs_df.iloc[start_idx:end_idx]

    def get_segment_slice(
        self, flt_legs_df: pd.DataFrame, seg_origin: str, seg_destination: str
    ) -> Tuple[int, int]:
        """return start and end row indexes (all rows of legs belong to a leg)"""
        start_idx = (flt_legs_df.leg_origin == seg_origin).idxmax()
        end_idx = (
            flt_legs_df[
                flt_legs_df.leg_destination == seg_destination
            ].last_valid_index()
            + 1
        )
        return start_idx, end_idx

    def get_flight_segments(self, flt_df: pd.DataFrame) -> pd.DataFrame:
        """get all possible segments for a fligth"""
        return flt_df.drop_duplicates(["seg_origin", "seg_destination"])

    def parse(self) -> List[_Segment]:
        try:
            """parse csv fiels and return list of _Segment objects"""
            data = []
            for flt_g, flt_df in self.segments.groupby(
                ["airline_code", "flt_number", "flt_number_ext", "flt_dept_date"]
            ):
                al_code, flt_number, flt_number_ext, flt_dept_date = flt_g
                segments_df = self.get_flight_segments(flt_df)
                data.extend(
                    self.__handle_segments(
                        al_code, flt_number, flt_number_ext, flt_dept_date, segments_df
                    )
                )

            return data
        except Exception as e:
            self.logger.error(str(e) + "(daily_csv_handler)")
            raise Exception(str(e))

    def __init__(self, date, path_to_files=HITIT_FILES_PATH):
        dt = date if type(date) is int else int(date.replace("-", ""))
        self.date = dt
        self.path_to_files = path_to_files
        self.logger = Logger("logs")
        self.all_files_are_available()
        self.legs = self.load_legs()
        self.segments = self.load_segments()

    def all_files_are_available(self) -> bool:
        """
        hitit handler needs 3 files combined together to get all data we need
        1 - SEGCLSREV files
        2 - LEGCLSRES files
        3 - FLTLEGINV files
        if one or more of these files is missing this method should return False otherwise it will return True
        """

        files = [
            f"{self.seg_cls_path}.csv",
            f"{self.seg_cls_path}.tag",
            f"{self.leg_cls_path}.csv",
            f"{self.leg_cls_path}.tag",
            f"{self.flt_leg_path}.csv",
            f"{self.flt_leg_path}.tag",
        ]

        missing_files = [filename for filename in files if not Path(filename).exists()]
        if missing_files:
            self.handle_files_missing(missing_files)
        return len(missing_files) == 0

    def handle_files_missing(self, missing_files: List[str]):
        """write logic to handle missing files case"""
        self.logger.warning(
            f"missing files : {', '.join(missing_files)}, (daily_csv_handler)"
        )

    def handle_empty_file(self, filename: str):
        """write logic to handle empty file case"""
        self.logger.warning(f"empty file({filename}), daily_csv_handler")

    def load_legs(self) -> pd.DataFrame:
        """
        merge 2 files to get legs data : LEGCLSRES and FLTLEGINV files
        """
        first = pd.read_csv(f"{self.leg_cls_path}.csv", names=self.leg_cls_cols)
        second = pd.read_csv(f"{self.flt_leg_path}.csv", names=self.flt_leg_cols)
        first.fillna("-", inplace=True)
        second.fillna("-", inplace=True)

        if first.empty:
            self.handle_empty_file(f"{self.leg_cls_path}.csv")
        if second.empty:
            self.handle_empty_file(f"{self.flt_leg_cols}.csv")

        merged = second.merge(
            first,
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
        return merged

    def load_segments(self) -> pd.DataFrame:
        """get segments data from SEGCLSREV files"""
        seg_df = pd.read_csv(f"{self.seg_cls_path}.csv", names=self.seg_cls_cols)
        if seg_df.empty:
            self.handle_empty_file(f"{self.seg_cls_path}.csv")
        seg_df.fillna("-", inplace=True)
        return seg_df
