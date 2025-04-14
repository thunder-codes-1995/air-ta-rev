from typing import Any, Dict, List, Optional

from base.repository import BaseRepository


class FlightInventoryRepository(BaseRepository):
    collection = "hitit"

    def get_load_factor(
        self,
        match: Optional[Dict[str, Any]] = None,
        cabin: Optional[List[str]] = None,
        extended: bool = False,
        only_latest: bool = False,
    ):

        project = {
            "_id": 0,
            "dept_date": {
                "$concat": [
                    {
                        "$substr": ["$string_dept_date", 0, 4],
                    },
                    "-",
                    {
                        "$substr": ["$string_dept_date", 4, 2],
                    },
                    "-",
                    {
                        "$substr": ["$string_dept_date", 6, 2],
                    },
                ],
            },
            "lf": "$legs.cabins.load_factor",
            "cabin": "$legs.cabins.code",
            "flt_num": "$flight_number",
            "dept_time": "$legs.departure_time",
            "origin": "$legs.origin",
            "destination": "$legs.destination",
            "inserted_date": "$date",
            "inserted_at": "$inserted_at",
            "airline_code": "$airline_code",
            "market": {"$concat": ["$origin", "-", "$destination"]},
            "date": 1,
        }

        if extended:
            project = {
                **project,
                "cap": {"$cond": {"if": {"$eq": ["-", "$legs.cabins.capacity"]}, "then": None, "else": "$legs.cabins.capacity"}},
                "total_booking": {
                    "$cond": {
                        "if": {"$eq": ["-", "$legs.cabins.total_booking"]},
                        "then": None,
                        "else": "$legs.cabins.total_booking",
                    }
                },
            }

        pipeline = [
            {
                "$match": match,
            },
            {
                "$unwind": {
                    "path": "$legs",
                },
            },
            {
                "$unwind": {
                    "path": "$legs.cabins",
                },
            },
            {
                "$addFields": {
                    "string_dept_date": {
                        "$toString": "$legs.departure_date",
                    },
                },
            },
            {"$project": project},
            {"$sort": {"date": -1}},
        ]

        if cabin:
            pipeline.insert(
                3,
                {
                    "$match": {
                        "legs.cabins.code": {"$in": cabin},
                    },
                },
            )

        if only_latest:
            pipeline.extend(
                [
                    {
                        "$group": {
                            "_id": {
                                "origin": "$origin",
                                "destination": "$destination",
                                "flt_num": "$flt_num",
                                "dept_date": "$dept_date",
                                "airline_code": "$airline_code",
                                "dept_time": "$legs.departure_time",
                                "market": "$market",
                                "cabin": "$cabin",
                            },
                            "data": {
                                "$push": {
                                    "lf": "$lf",
                                    "inserted_date": "$inserted_date",
                                    "inserted_at": "$inserted_at",
                                    "cap": "$cap",
                                    "total_booking": "$total_booking",
                                    "date": "$date",
                                }
                            },
                        }
                    },
                    {
                        "$addFields": {
                            "data": {"$first": "$data"},
                        }
                    },
                    {
                        "$project": {
                            "_id": 0,
                            "market": "$_id.market",
                            "origin": "$_id.origin",
                            "destination": "$_id.destination",
                            "flt_num": "$_id.flt_num",
                            "dept_date": "$_id.dept_date",
                            "airline_code": "$_id.airline_code",
                            "cabin": "$_id.cabin",
                            "dept_time": "$_id.legs.departure_time",
                            "lf": "$data.lf",
                            "inserted_date": "$data.inserted_date",
                            "inserted_at": "$data.inserted_at",
                            "date": "$data.date",
                            "cap": {"$cond": {"if": {"$eq": ["-", "$data.cap"]}, "then": None, "else": "$data.cap"}},
                            "total_booking": {
                                "$cond": {
                                    "if": {"$eq": ["-", "$data.total_booking"]},
                                    "then": None,
                                    "else": "$data.total_booking",
                                }
                            },
                        }
                    },
                ],
            )

        return self.aggregate(pipeline)
