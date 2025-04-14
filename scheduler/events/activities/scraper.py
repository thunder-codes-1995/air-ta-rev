import json
import os
import re
import sys
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
project_root = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(project_root, '..', '..'))
sys.path.append(project_root)
from events import EventBase

class ActivityEvent(EventBase):

    def __init__(self, country):
        super().__init__(country)
        self.url = f"https://www.eventbrite.com{country['href']}"

    def convert_date(self, date_str):
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        return date_obj

    def get_holidays(self):
        dates = [datetime.now() + timedelta(days=i) for i in range(1, 6)]
        dates_str = [date.strftime("%Y-%m-%d") for date in dates]

        events = []

        for date in dates_str:
            endpoint = f"{self.url}?start_date={date}&end_date={date}"
            soup = BeautifulSoup(self.get_page(endpoint), 'html.parser')
            if soup.text.find("Whoops") != -1:
                continue
            divs = soup.select(".search-event-content")
            for div in divs:
                title = div.select("h2")[0].text
                date_obj = self.convert_date(date)
                all_str = f"{date_obj.strftime('%Y-%m-%d')} {title} {self.country_code}".lower().replace(
                    " ", "_")
                events.append(
                    {
                        "event_year": date_obj.year,
                        "start_month": date_obj.month,
                        "end_month": date_obj.month,
                        "country_code": self.country_code,
                        "start_date": int(date_obj.strftime('%Y%m%d')),
                        "end_date": int(date_obj.strftime('%Y%m%d')),
                        "dow": date_obj.strftime('%A'),
                        "all_str": all_str,
                        "event_idx": self.generate_64_char_uuid(),
                        "event_name": title,
                        "categories": [
                            {"category": "concerts", "sub_categories": []}
                        ],
                        "city": None
                    }
                )

        return events



if __name__ == '__main__':
    with open("countries.json", "r") as json_file:
        data = json.load(json_file)

        for item in data:
            event = ActivityEvent(country=item)
            event.save_events()
