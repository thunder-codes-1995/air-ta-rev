import re
from datetime import datetime
from typing import Dict, TypedDict, Optional

from pydantic import BaseModel, model_validator, validator

from airports.repository import AirportRepository
from events.repository import EventRepository

event_reop = EventRepository()
airport_repo = AirportRepository()


def date_competiable(value: str) -> bool:
    date_pattern = re.compile(r"^\d{4}-\d{2}-\d{2}$")
    return bool(date_pattern.match(value))


def none_empty(value: str) -> bool:
    return bool(value.strip())


class UpdateData(TypedDict):
    start_date: str
    end_date: str
    name: str


class CreateData(UpdateData):
    country_code: str
    city_code: str


class UpdateEvent(BaseModel):
    start_date: str
    end_date: str
    name: str
    category: str
    sub_category: str
    city_code: Optional[str] = None
    country_code: Optional[str] = None

    @model_validator(mode="before")
    @classmethod
    def validate_date(cls, data: Dict[str, str]) -> Dict[str, str]:
        if not date_competiable(data["start_date"]):
            raise ValueError(f"{data['start_date']} is not date competiable (YYYY-MM-DD)")

        if datetime.strptime(data["start_date"], "%Y-%m-%d").date() > datetime.strptime(data["end_date"], "%Y-%m-%d").date():
            raise ValueError(f"start date {data['start_date']} cannot be greater than end value {data['end_date']}")

        if not date_competiable(data["end_date"]):
            raise ValueError(f"{data['end_date']} is not date competiable (YYYY-MM-DD)")

        if datetime.strptime(data["end_date"], "%Y-%m-%d").date() < datetime.strptime(data["start_date"], "%Y-%m-%d").date():
            raise ValueError(f"end date {data['end_date']} cannot be less than end value {data['start_date']}")

        return data

    @validator("name")
    def validate_name(cls, value: str) -> str:
        if not none_empty(value):
            raise ValueError(f"name value cannot be empty")

        return value

    @validator("category")
    def validate_category(cls, value: str) -> str:
        unqiue_categories = list(map(lambda item: item["value"], event_reop.categories_unique()))

        if value not in unqiue_categories:
            raise ValueError(f"invalid category")

        if not none_empty(value):
            raise ValueError(f"category value cannot be empty")

        return value

    @validator("sub_category")
    def validate_sub_category(cls, value: str) -> str:
        unqiue_sub_categories = list(map(lambda item: item["value"], event_reop.sub_category_unique()))


        if not none_empty(value):
            return value

        if value not in unqiue_sub_categories:
            raise ValueError(f"invalid sub-category")


        return value

    def validate_city_or_country(self, markets):

        if not self.city_code and not self.country_code:
            raise ValueError("At least one city code or country code must be entered.")

        if self.city_code:
            country = airport_repo.get_country_code_by_city_code(self.city_code)
            if not airport_repo.is_valid_city_code(self.city_code):
                raise ValueError(f"invalid city code {self.city_code}")

            if self.country_code:
                if country != self.country_code:
                    raise ValueError(f"The selected city ({self.city_code}) does not belong to this country ({self.country_code})")

            if self.city_code not in markets:
                raise ValueError(f"This city ({self.city_code}) is not among the client's markets")

        if self.country_code:
            if not airport_repo.is_valid_country_code(self.country_code):
                raise ValueError(f"invalid country code ({self.country_code})")
        else:
            self.country_code = country

    def data(self, markets) -> UpdateData:
        market_cities = []
        for market in markets:
            market_cities.extend([market["orig"], market["dest"]])

        self.validate_city_or_country(markets=list(set(market_cities)))
        return {
            "city_code": self.city_code,
            "country_code": self.country_code,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "name": self.name,
            "category": self.category.strip(),
            "sub_category": self.sub_category.strip(),
        }

class CreateMultipleEvents(UpdateEvent):

    def data(self, markets):
        events = []
        template = {
            "start_date": self.start_date,
            "end_date": self.end_date,
            "name": self.name,
            "category": self.category,
            "sub_category": self.sub_category
        }
        # if city_code provided ignore country
        if self.city_code:
            for city in self.city_code.split(","):
                template.update({"city_code": city.strip(), "country_code": None})
                event = CreateEvent(**template).data(markets=markets)
                events.append(event)
        else:
            if self.country_code:
                for country in self.country_code.split(","):
                    template.update({"city_code": None, "country_code": country.strip()})
                    event = CreateEvent(**template).data(markets=markets)
                    events.append(event)

        return events



class CreateEvent(UpdateEvent):

    def data(self, markets) -> CreateData:
        market_cities = []
        for market in markets:
            market_cities.extend([market["orig"], market["dest"]])

        self.validate_city_or_country(markets=list(set(market_cities)))


        return {
            "city_code": self.city_code,
            "country_code": self.country_code,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "name": self.name,
            "category": self.category.strip(),
            "sub_category": self.sub_category.strip(),
        }
