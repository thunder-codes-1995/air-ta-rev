from dataclasses import dataclass

from events.filterbymarket.fields import EVENT_FIELDS
from events.filterbymarket.form import FilterByMarketForm
from events.filterbymarket.data import FilterEventData
from events.repository import EventRepository


events_repo = EventRepository()

def get_event_category(data):
    first_cat = data['categories'][0]['category']
    if data['categories'][0]['sub_categories']:
        sub_cat = data['categories'][0]['sub_categories'][0]
    else:
        sub_cat = None
    return first_cat, sub_cat

def prepare_response(events):
    res = {
        "data": [],
        "labels": EVENT_FIELDS,
    }

    for event in events:
        category, sub_category = get_event_category(event)

        res["data"].append({
            "id": str(event["id"]),
            "event_name": event["event_name"],
            "category": category,
            "sub_category": sub_category,
            "country_code": event["country_code"],
            "city_code": event.get("city") or None,
            "start_date": event["str_start_date"],
            "end_date": event["str_end_date"],
        })
        return res

@dataclass
class FilterEventByMarket:
    form: FilterByMarketForm
    host_code: str

    def get(self):
        events = FilterEventData(self.form, self.host_code).get()
        res = prepare_response(events)

        return res

