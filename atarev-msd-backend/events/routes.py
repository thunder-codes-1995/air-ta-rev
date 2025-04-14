from enum import Enum


class Route(Enum):
    ALERTS = "alerts"
    EVENT_TABLE = "table"
    EVENT_CALENDAR = "calendar"
    EVENT_FIELDS = "fields"
    GET_PAX_BOOKING_TRENDS = "pax-trends"
    EVENT_DEMAND = "demand"
    GET_EVENT_STORE_OPTIONS = "options"
    FILTER_BY_MARKET = "filter-by-market"
    EVENT_GROUP = "group"
