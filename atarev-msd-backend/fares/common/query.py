from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from base.helpers.user import User
from base.mongo_utils import convert_list_param_to_criteria, merge_criterions


@dataclass
class Match:
    user: User
    origin: List[str]
    destination: List[str]
    flight_type: str
    graph_carriers: Optional[List[str]] = None
    connections: Optional[List[int]] = None
    weekdays: Optional[List[int]] = None
    duration: Optional[int] = None
    date_range: Optional[Tuple[int, int]] = None
    time_range: Optional[Tuple[int, int]] = None
    date: Optional[int] = None

    @classmethod
    def handle_flight_keys(cls, key: str, flight_keys: List[str]) -> List[Any]:
        return [{"$and": [{key: int(flt_key[2:])}, {"carrierCode": flt_key[0:2]}]} for flt_key in flight_keys]

    def __handle_connections(self) -> Dict[str, Any]:
        if self.connections:
            return {"$or": [{"itineraries.0.legs": {"$size": connection + 1}} for connection in self.connections]}

        return {}

    @property
    def query(self):

        _and = [self.__handle_connections()]
        match = merge_criterions(
            [
                convert_list_param_to_criteria("marketOrigin", self.origin),
                convert_list_param_to_criteria("marketDestination", self.destination),
                convert_list_param_to_criteria("carrierCode", self.graph_carriers),
                convert_list_param_to_criteria("outboundDayOfWeek", self.weekdays),
                convert_list_param_to_criteria("type", self.flight_type),
                {"hostCode": self.user.carrier},
            ]
        )

        if self.duration:
            match = {**match, "itineraries.0.itinDuration": {"$gte": self.duration}}

        if self.date_range:
            _and = [
                *_and,
                *[
                    {"outboundDate": {"$gte": self.date_range[0]}},
                    {"outboundDate": {"$lte": self.date_range[1]}},
                ],
            ]

        if self.time_range:
            _and = [
                *_and,
                *[
                    {"itineraries.0.itinDeptTime": {"$gte": self.time_range[0]}},
                    {"itineraries.0.itinDeptTime": {"$lte": self.time_range[1]}},
                ],
            ]

        if self.date:
            match = {**match, "outboundDate": self.date}

        if _and:
            match["$and"] = _and

        return match


@dataclass
class LoadFactorQuery:
    host_code: str
    origin: List[str]
    destination: List[str]
    start_date: Optional[int] = None
    end_date: Optional[int] = None
    departure_date: Optional[int] = None

    @property
    def query(self):
        match = merge_criterions(
            [
                convert_list_param_to_criteria("airline_code", self.host_code),
                convert_list_param_to_criteria("origin", self.origin),
                convert_list_param_to_criteria("destination", self.destination),
            ]
        )

        if self.start_date and self.end_date:
            match["$and"] = [
                {"departure_date": {"$gte": self.start_date}},
                {"departure_date": {"$lte": self.end_date}},
            ]

        if self.departure_date:
            match["departure_date"] = self.departure_date

        return match
