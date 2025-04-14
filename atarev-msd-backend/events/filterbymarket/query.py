from typing import Any, Dict, List
from dataclasses import dataclass

from events.repository import EventRepository

event_repository = EventRepository()



@dataclass
class EventFilterQuery:
    country_code: List[str]
    airline_code: str
    date:int

    @property
    def query(self) -> Dict[str, Any]:
        query = {"country_code": {"$in": list(self.country_code)},
                 "airline_code": self.airline_code,
                 "start_date": {"$lte": self.date},
                 "end_date": {"$gte": self.date}}

        return query

