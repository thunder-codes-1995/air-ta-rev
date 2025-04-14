from typing import List

from flask import request

from base.helpers.user.actions import UserAction
from base.service import BaseService
from events.alerts.data import AlertData
from events.booking_trends import PaxBookingTrends
from events.booking_trends.form import BookingTrendsForm
from events.calendar import EventCalendar
from events.calendar.form import EventCalendarForm
from events.common.fields import FieldGroup
from events.crud import RemoveEvent, StoreEvent
from events.crud.action import Action
from events.demand import EventDemand
from events.demand.form import EventDemandForm
from events.fields import EventField
from events.fields.form import EventFieldForm
from events.filterbymarket import FilterByMarketForm, FilterEventByMarket
from events.group import GroupedEvents
from events.options import Option
from events.table import EventTable
from events.table.form import EventTableForm


class EventService(BaseService):

    def get_store_options(self):
        return Option(request, host_code=request.user.carrier).get()

    def get_demand(self, form: EventDemandForm):
        return EventDemand(form, request.user.carrier).get()

    def get_pax_booking_trends(self, form: BookingTrendsForm):
        return PaxBookingTrends(form, request.user.carrier).get()

    def get_event_table(self, form: EventTableForm):
        # UserAction(request.user).add_value("event_table_fields", form.get_selected_fields())
        return EventTable(form, request.user).get()

    def filter_by_market(self, form: FilterByMarketForm):
        return FilterEventByMarket(form, request.user.carrier).get()

    def get_grouped_events(self):
        return GroupedEvents(request.user.carrier).get()

    def get_event_calendar(self, form: EventCalendarForm):
        return EventCalendar(form, request.user.carrier, request.user).get()

    def store_event(self) -> bool:
        action: str = Action.CREATE.value if request.method == "POST" else Action.UPDATE.value
        return StoreEvent(request, action, request.json, request.user.carrier).store()

    def delete_event(self) -> bool:
        return RemoveEvent(request.json, request.user.carrier).delete()

    def event_fields(self, form: EventFieldForm) -> List[FieldGroup]:
        return EventField(form=form, user=request.user).get()

    def get_event_alerts(self):
        return AlertData(request.user.carrier).get()
