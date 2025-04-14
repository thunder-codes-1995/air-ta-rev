from dataclasses import dataclass

from airports.repository import AirportRepository
from events.filterbymarket import FilterByMarketForm
from datetime import datetime

from events.filterbymarket.query import event_repository, EventFilterQuery

@dataclass
class FilterEventData:
    form: FilterByMarketForm
    host_code: str

    def get(self):
        origin_list = self.form.get_origins()
        destination_list = self.form.get_destinations()
        airports = origin_list + destination_list
        airport_repository = AirportRepository()
        country_map = airport_repository.get_countries_for_airports(airports=airports)
        countries = set(country_map.values())

        date = datetime.strptime(self.form.data["date"], '%Y-%m-%d')
        date_int = int(date.strftime('%Y%m%d'))

        qr = EventFilterQuery(country_code=list(countries), airline_code=self.host_code,
                              date=date_int).query

        events = list(event_repository.get_events(match=qr))
        return events
