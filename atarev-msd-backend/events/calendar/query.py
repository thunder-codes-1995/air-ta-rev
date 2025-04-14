from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Tuple

from base.entities.carrier import CityBasedMarket
from base.helpers.fields import Field
from base.helpers.user import User
from base.mongo_utils import convert_list_param_to_criteria, merge_criterions
from events.common.query import EventQuery
from fares.repository import FareRepository
from users.repository import UserRepository

from .form import EventCalendarForm

user_repository = UserRepository()


@dataclass
class EventCalendarQuery:
    host_code: str
    form: EventCalendarForm
    user: User

    @property
    def query(self) -> Dict[str, Any]:

        user = user_repository.get_user(self.user.username)
        event_ids = user.get("selected_filter_options", {}).get("selected_event_ids", [])

        match = EventQuery(
            host_code=self.host_code,
            location=self.form.get_origin() + self.form.get_destination(),
            is_country_market=self.form.is_country_market(),
            category=self.form.get_category(),
            sub_category=self.form.get_sub_category(),
            event_ids=event_ids,
        ).query

        return {**match, **self.__date_match()}

    def __date_match(self) -> Dict[str, Any]:
        if self.form.month.data:
            today = datetime.now()
            year = today.year
            month = self.form.month.data
            return {"$or": [{"event_year": year, "start_month": month}, {"event_year": year - 1, "start_month": month}]}

        dt_range = self.form.get_date_range()
        return {
            "$and": [
                {"start_date": {"$gte": dt_range[0]}},
                {"start_date": {"$lte": dt_range[1]}},
            ],
        }


@dataclass
class CalendarFareQuery:
    form: EventCalendarForm
    host_code: str
    comp_code: str
    date_range: Tuple[int, int]

    @property
    def query(self):
        origin = self.form.get_origin()[0]
        destination = self.form.get_destination()[0]
        markets = CityBasedMarket(self.host_code, origin, destination).airport_based_markets()

        match = merge_criterions(
            [
                {"hostCode": self.host_code},
                convert_list_param_to_criteria("carrierCode", [self.host_code, self.comp_code]),
                # fares for certain clients are stored using cities instead of airports
                # if markets is empty use city code instead,otherwise the query will take forever
                convert_list_param_to_criteria("marketOrigin", list(set([market["origin"] for market in markets])) or [origin]),
                convert_list_param_to_criteria(
                    "marketDestination", list(set([market["destination"] for market in markets])) or [destination]
                ),
            ]
        )

        return [
            {
                "$match": {
                    **match,
                    "$and": [
                        {"outboundDate": {"$gte": self.date_range[0]}},
                        {"outboundDate": {"$lte": self.date_range[1]}},
                    ],
                }
            },
            {"$unwind": {"path": "$lowestFares"}},
            {"$match": {"$or": FareRepository.create_expiry_fares_date_match()}},
            {
                "$project": {
                    "_id": 0,
                    "fare": "$lowestFares.fareAmount",
                    "currency": "$lowestFares.fareCurrency",
                    "outboundDate": 1,
                    "carrierCode": 1,
                    "cabin": "$lowestFares.cabinName",
                }
            },
            {"$sort": {"fare": 1}},
            {
                "$group": {
                    "_id": {"departure_date": "$outboundDate", "carrier_code": "$carrierCode"},
                    "fares": {"$push": {"fare": "$fare", "currency": "$currency", "cabin": "$cabin"}},
                }
            },
            {"$addFields": {"data": {"$first": "$fares"}}},
            {
                "$project": {
                    "_id": 0,
                    "departure_date": "$_id.departure_date",
                    "carrier_code": "$_id.carrier_code",
                    "fare": "$data.fare",
                    "currency": "$data.currency",
                    "cabin": "$data.cabin",
                    **Field.date_as_string("str_departure_date", "_id.departure_date"),
                }
            },
        ]
