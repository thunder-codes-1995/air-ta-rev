from collections import defaultdict
from dataclasses import dataclass

from events.filterbymarket.query import event_repository

def get_event_category(data):
    first_cat = data['categories'][0]['category']
    if data['categories'][0]['sub_categories']:
        sub_cat = data['categories'][0]['sub_categories'][0]
    else:
        sub_cat = None
    return first_cat, sub_cat

def prepare_event(event):
    category, sub_category = get_event_category(event)
    return {
        "id": str(event["id"]),
        "event_name": event["event_name"],
        "category": category,
        "sub_category": sub_category,
        "country_code": event["country_code"],
        "city_code": event.get("city") or None,
        "start_date": event["str_start_date"],
        "end_date": event["str_end_date"],
    }

@dataclass
class GroupEventData:
    host_code: str

    def get(self):
        events = list(event_repository.get_events({"airline_code": self.host_code}))

        category_events = defaultdict(lambda: defaultdict(list))

        # Group events by category and sub_category
        for event in events:
            for category in event['categories']:
                category_name = category['category']
                if len(category["sub_categories"]) != 0 and len("".join(category["sub_categories"])) != 0:
                    for sub_category in category['sub_categories']:
                        category_events[category_name][sub_category].append(prepare_event(event))
                else:
                    category_events[category_name]['__all__'].append(prepare_event(event))

        return category_events
