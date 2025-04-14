import os
from datetime import datetime, timedelta
from typing import List, Tuple

import pandas as pd
from attr import asdict
from core.db import DB
from core.errors import HititIntervalException
from core.logger import Logger
from models.schedule import Class
from pymongo import UpdateOne
from pymongo.cursor import Cursor
from typs import UpdateInventoryParams
from zeep import Client


class UpdateInventory:
    db = DB()
    logger = Logger("logs")

    def __init__(self, url: str, params):
        self.args = UpdateInventoryParams(**params)
        self.url = url

    def __get(self):
        """connect to web service and get data"""
        client = Client(self.url)
        resp = client.service.GetRevenueManagement(
            **{
                "RevenueManagementOperationRequest": {
                    "clientInformation": {
                        "member": False,
                        "userName": os.getenv("HITIT_USERNAME"),
                        "password": os.getenv("HITIT_PASSWORD"),
                    },
                    **asdict(self.args),
                }
            }
        )

        return resp

    def __handle_new_segments(self) -> pd.DataFrame:
        """convert web service response into comparable df"""
        resp = self.__get()
        if resp is None:
            self.logger.info("hitit update inventory returned None, we need to wait 5 more minutes")
            raise HititIntervalException("Please wait 5 minutes to make next request")

        data = []

        for obj in resp.flightSegmentRMList:
            classes = []
            for cls in obj.bookingClassList:
                classes.append(
                    {
                        "code": cls.bookingClass,
                        "cabin": cls.cabinCode,
                        "seats_available": cls.seatsAvailable,
                        "display_sequence": cls.displaySequence,
                        "total_booking": cls.totalBookings,
                        "total_group_booking": cls.totalGroupBookings,
                        "flt_num": str(int(obj.fltNo)),
                        "seg_dep_date": int(obj.segDepDate),
                        "seg_destination": obj.segDestination,
                        "seg_origin": obj.segOrigin,
                    }
                )
            data.extend(classes)

        return pd.DataFrame(
            data,
            columns=[
                "code",
                "cabin",
                "seats_available",
                "display_sequence",
                "total_booking",
                "total_group_booking",
                "flt_num",
                "seg_dep_date",
                "seg_destination",
                "seg_origin",
            ],
        )

    def __handle_existing_segments(self) -> pd.DataFrame:
        """convert list of existing segments into comparable df"""
        cursor = self.get_existing_segments()
        data = []

        for segment in cursor:
            for leg in segment["legs"]:
                for cabin in leg["cabins"]:
                    for cls in cabin["classes"]:
                        cls["cabin"] = cabin["code"]
                        cls["flt_num"] = segment["flight_number"]
                        cls["seg_dep_date"] = segment["departure_date"]
                        cls["seg_destination"] = segment["destination"]
                        cls["seg_origin"] = segment["origin"]
                    data.extend(cabin["classes"])

        return pd.DataFrame(
            data,
            columns=[
                "code",
                "seats_available",
                "total_booking",
                "total_group_booking",
                "parent_booking_class",
                "authorization",
                "cabin",
                "flt_num",
                "seg_dep_date",
                "seg_destination",
                "seg_origin",
            ],
        )

    def get_existing_segments(self) -> Cursor:
        start_date_int, end_date_int = self.__get_date_range()
        return self.db.schedule.find(
            {
                "$and": [
                    {"departure_date": {"$gte": start_date_int}},
                    {"departure_date": {"$lte": end_date_int}},
                ]
            }
        )

    def __get_date_range(self) -> Tuple[int, int]:
        """get existing segments based on range return by this method"""
        days = self.args.dayCount or 30
        today = datetime.today()
        start_date_string = self.args.startDate or today.strftime("%Y%m%d")
        end_date_string = (datetime.strptime(start_date_string, "%Y-%m-%d") + timedelta(days=days)).strftime("%Y-%m-%d")

        start_date_int = int(start_date_string.replace("-", ""))
        end_date_int = int(end_date_string.replace("-", ""))

        return start_date_int, end_date_int

    def update(self) -> None:
        """
        - get existing segments from db
        - get new segments using web service
        - convert buth results to comparable dfs
        - merge 2 dfs and get the updated version
        - store result
        """
        current_segments_df = self.__handle_existing_segments()
        new_segments_df = self.__handle_new_segments()

        if not self.can_update(current_segments_df, new_segments_df):
            return

        merged = current_segments_df.merge(
            new_segments_df,
            on=[
                "cabin",
                "flt_num",
                "seg_dep_date",
                "seg_destination",
                "seg_origin",
                "code",
            ],
            suffixes=("_left", "_right"),
        )

        merged = merged[
            [
                "code",
                "seats_available_right",
                "total_booking_right",
                "total_group_booking_right",
                "parent_booking_class",
                "authorization",
                "cabin",
                "flt_num",
                "seg_dep_date",
                "seg_destination",
                "seg_origin",
                "display_sequence",
            ]
        ]
        self.__store(merged)

    # fmt: off
    def __store(self, updated_segments_df: pd.DataFrame) -> None:
        """ update segments and store into db """
        bulk = []
        cursor = self.get_existing_segments()

        for segment in cursor:
            updated_segment = self.__update_segment(segment, updated_segments_df)
            bulk.append(UpdateOne(
                        {'origin': segment['origin'], 'destination': segment['destination'],
                         'flight_number': segment['flight_number'], "departure_date": segment['departure_date']},
                        {"$set": updated_segment},
                        upsert=False))

        self.db.schedule.bulk_write(bulk)

    def __update_segment(self, segment, updated_segments_df: pd.DataFrame):
        """ 
            update classes for one segment doing the following steps : 
            - loop through all legs -> through all cabins 
            - for each cabin update clasess
            - update segment classes for each cabin 
            return updated segment 
        """
        updated_segment = {**segment}

        for leg in updated_segment['legs']:
            for cabin in leg['cabins']:
                classes = self.__update_classes(segment['origin'], segment['destination'], segment['departure_date'],
                                                segment['flight_number'], cabin['code'], updated_segments_df)
                # fmt: off
                if classes: cabin['classes'] = [cls.json for cls in classes]

        return updated_segment 


    def __update_classes(self, segment_origin: str, segment_destination: str, segment_dept_date: int,
                         segment_flt_num: str, cabin_code: str, updated_segments_df: pd.DataFrame) -> List[Class]:
        """ return list of updated Class objects """
        targeted_df = updated_segments_df[
            (updated_segments_df.seg_origin == segment_origin) &
            (updated_segments_df.seg_destination == segment_destination) &
            (updated_segments_df.seg_dep_date == segment_dept_date) &
            (updated_segments_df.flt_num == segment_flt_num) &
            (updated_segments_df.cabin == cabin_code)
        ]

        if targeted_df.empty:
            return []

        targeted_df = targeted_df.drop_duplicates(['code','seats_available_right','total_booking_right','total_group_booking_right',
                                     'parent_booking_class','authorization','cabin','flt_num','seg_dep_date','seg_destination','seg_origin'])

        targeted_df = targeted_df.sort_values("display_sequence")
        return [Class(**obj) for obj in targeted_df[['code', 'seats_available_right', 'total_booking_right', 'total_group_booking_right', 'parent_booking_class', 'authorization','display_sequence']].rename(columns={
            "seats_available_right": "seats_available",
            'total_booking_right': 'total_booking',
            'total_group_booking_right': "total_group_booking",
        }).to_dict("records")]

    def can_update(self, current_segments_df: pd.DataFrame, new_segments_df: pd.DataFrame) -> bool:
        """ check whether data can be updated or not """
        if current_segments_df.empty:
            self.logger.warning(f"no segments were found in db, parameters : {self.params_as_string}")
            return False

        if new_segments_df is None:
            self.logger.warning(f"hitit web service returned None : {self.params_as_string}")
            return False 

        if new_segments_df.empty:
            self.logger.warning(f"hitit web service returned Empty Data : {self.params_as_string}")
            return False

        return True

    @property
    def params_as_string(self) -> str:
        d = asdict(self.args)
        return ', '.join([f"{k} = {v}" for k, v in d.items()])
