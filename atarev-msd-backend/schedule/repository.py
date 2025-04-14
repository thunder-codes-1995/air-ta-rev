from typing import Dict, Optional, Union

from base.helpers.fields import Field
from base.repository import BaseRepository


class ScheduleRepository(BaseRepository):

    collection = "schedule"

    def get(
        self,
        match: Optional[Dict[str, Union[str, int]]] = None,
        cabin: Optional[str] = None,
        flatten: bool = False,
    ):

        project = {
            "_id": 0,
            "flt_num": "$opFltNum",
            "arrival_date": "$arrivalDate",
            "arrival_day": "$arrivalDay",
            "arrival_month": "$arrivalMonth",
            "departure_date": "$deptDate",
            "arrival_year": "$arrivalYear",
            "arrival_time": "$arrivalTime",
            "departure_time": "$deptTime",
            "departure_day": "$deptDay",
            "departure_month": "$deptMonth",
            "departure_year": "$deptYear",
            "origin": "$originCode",
            "destination": "$destCode",
            "carrier_code": "$opAlCode",
            "flt_key": "$flightKey",
            **Field.date_as_string("str_departure_date", "deptDate"),
            **Field.date_as_string("str_arrival_date", "arrivalDate"),
        }

        if cabin or flatten:
            pipeline = [
                {"$match": {**(match or {})}},
                {"$unwind": {"path": "$cabins"}},
                {"$match": ({"cabins.code": {"$regex": cabin, "$options": "i"}} if cabin else {})},
                {"$unwind": {"path": "$cabins.classes"}},
                {
                    "$project": {
                        "cabin": "$cabins.code",
                        "bkd_lf": "$cabins.bookedLF",
                        "exp_lf": "$cabins.expectedLF",
                        "avg_lf": "$cabins.averageLF",
                        "class": "$cabins.classes.code",
                        **project,
                    }
                },
            ]

        else:
            pipeline = [
                {"$match": {**(match or {})}},
                {"$project": {**project, "cabins": 1}},
            ]

        return self.aggregate(pipeline)
