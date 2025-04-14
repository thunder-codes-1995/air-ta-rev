import calendar
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, TypedDict
from uuid import uuid4


class Typ(TypedDict):
    category: str
    sub_categories: List[str]


class UpdateItemType(TypedDict):
    event_name: str
    start_month: int
    end_month: int
    event_year: int
    start_date: int
    end_date: int
    dow: str
    categories: List[Typ]


class CreateItemType(UpdateItemType):
    event_idx: str
    all_str: str
    airline_code: str
    country_code: str
    city: Optional[str]


@dataclass
class GenerateUpdateItem:
    start_date: str
    end_date: str
    category: str
    sub_category: str
    name: str
    country_code: Optional[str]
    city: Optional[str]

    def generate(self) -> UpdateItemType:
        sy, sm, sd = list(map(int, self.start_date.split("-")))
        ey, em, ed = list(map(int, self.end_date.split("-")))
        dow = calendar.day_name[datetime.strptime(self.start_date, "%Y-%m-%d").weekday()]

        return {
            "start_month": sm,
            "end_month": em,
            "event_year": sy,
            "start_date": int(f"{sy}{sm}{sd}"),
            "end_date": int(f"{ey}{em}{ed}"),
            "dow": dow,
            "event_name": self.name,
            "categories": self.__create_categories(self.category, self.sub_category),
            "city": self.city or None,
            "country_code": self.country_code or None,
            "all_str": "_".join([self.start_date, self.name.strip().lower().replace(" ", "-")]),
        }

    def __create_categories(self, category: str, sub_category: str) -> List[Typ]:
        return [{"category": category, "sub_categories": [sub_category]}]


@dataclass
class GenerateCreateItem(GenerateUpdateItem):

    airline_code: str

    def generate(self) -> CreateItemType:
        return {
            "event_idx": str(uuid4()),
            "airline_code": self.airline_code,
            "city": self.city or None,
            "country_code": self.country_code or None,
            **super().generate(),
        }
