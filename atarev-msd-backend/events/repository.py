from typing import Any, Dict, Iterable, List, Optional, TypedDict, Union

from base.helpers.fields import Field
from base.repository import BaseRepository


class Category(TypedDict):
    category: str
    sub_categories: List[str]


class UniqueCategory(TypedDict):
    value: str


class Event(TypedDict):
    categories: List[Category]
    city: Union[str, None]
    country_code: str
    event_name: str
    event_year: int
    event_month: int
    start_date: int
    end_date: int
    start_month: int
    dow: int
    event_idx: str
    str_start_date: str
    str_end_date: str


class EventRepository(BaseRepository):
    collection = "events"

    def get_events(self, match: Optional[Dict[str, Any]] = None) -> Iterable[Event]:

        pipelines = [
            {
                "$match": match or {},
            },
            {"$addFields": {"str_start_date": {"$toString": "$start_date"}, "str_end_date": {"$toString": "$end_date"}}},
            {
                "$project": {
                    "_id": 0,
                    "id": str("$_id"),
                    "categories": 1,
                    "city": 1,
                    "country_code": 1,
                    "event_name": 1,
                    "event_year": 1,
                    "event_month": 1,
                    "start_date": 1,
                    "end_date": 1,
                    "start_month": 1,
                    "dow": 1,
                    "event_idx": 1,
                    **Field.date_as_string("str_start_date", "start_date"),
                    **Field.date_as_string("str_end_date", "end_date"),
                }
            },
        ]

        return self.aggregate(pipelines)

    def categories_unique(self) -> Iterable[UniqueCategory]:
        agg = [
            {"$project": {"category": "$categories.category"}},
            {"$unwind": {"path": "$category"}},
            {"$group": {"_id": "$category"}},
            {"$project": {"_id": 0, "value": "$_id"}},
        ]

        return self.aggregate(agg)

    def sub_category_unique(self) -> Iterable[UniqueCategory]:
        agg = [
            {"$project": {"categories": 1}},
            {"$unwind": {"path": "$categories"}},
            {"$project": {"sub_category": "$categories.sub_categories"}},
            {"$unwind": {"path": "$sub_category"}},
            {"$group": {"_id": "$sub_category"}},
            {"$project": {"value": "$_id", "_id": 0}},
        ]

        return self.aggregate(agg)
