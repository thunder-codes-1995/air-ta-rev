from flask import request

from base.constants import Constants
from base.controller import BaseController
from events.booking_trends.form import BookingTrendsForm
from events.calendar.form import EventCalendarForm
from events.demand.form import EventDemandForm
from events.fields.form import EventFieldForm
from events.filterbymarket import FilterByMarketForm
from events.routes import Route
from events.service import EventService
from events.table.form import EventTableForm
from utils.funcs import create_error_response

repository = EventService()


class EventController(BaseController):
    def get(self, endpoint: str):
        form = super().get(endpoint)

        if endpoint == Route.EVENT_TABLE.value:
            return repository.get_event_table(form)

        if endpoint == Route.EVENT_CALENDAR.value:
            return repository.get_event_calendar(form)

        if endpoint == Route.EVENT_FIELDS.value:
            return repository.event_fields(form)

        if endpoint == Route.GET_PAX_BOOKING_TRENDS.value:
            return repository.get_pax_booking_trends(form)

        if endpoint == Route.EVENT_DEMAND.value:
            return repository.get_demand(form)

        if endpoint == Route.GET_EVENT_STORE_OPTIONS.value:
            return repository.get_store_options()

        if endpoint == Route.FILTER_BY_MARKET.value:
            return repository.filter_by_market(form)

        if endpoint == Route.EVENT_GROUP.value:
            return repository.get_grouped_events()

        if endpoint == Route.ALERTS.value:
            return repository.get_event_alerts()

        return create_error_response(Constants.ERROR_CODE_NOT_FOUND, "Invalid URL", 400)

    def get_validation_class(self):
        endpoint = request.path.split("/")[-1]

        if endpoint == Route.EVENT_TABLE.value:
            return EventTableForm

        if endpoint == Route.EVENT_CALENDAR.value:
            return EventCalendarForm

        if endpoint == Route.GET_PAX_BOOKING_TRENDS.value:
            return BookingTrendsForm

        if endpoint == Route.EVENT_DEMAND.value:
            return EventDemandForm

        if endpoint == Route.EVENT_FIELDS.value:
            return EventFieldForm

        if endpoint == Route.FILTER_BY_MARKET.value:
            return FilterByMarketForm

        return super().get_validation_class()

    def put(self, endpoint: str):
        return repository.store_event()

    def post(self, endpoint: str):
        return repository.store_event()

    def delete(self, endpoint: str):
        return repository.delete_event()
