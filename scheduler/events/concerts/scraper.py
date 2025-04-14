import json
import os
import re
import sys
from datetime import datetime
from bs4 import BeautifulSoup
project_root = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(project_root, '..', '..'))
sys.path.append(project_root)
from events import EventBase


class ConcertEvent(EventBase):

    def __init__(self, country):
        super().__init__(country)
        self.url = f"https://concertful.com{country['href']}"

    def convert_date(self, date_str):
        date_obj = datetime.strptime(date_str, '%a %b %d %Y')
        return date_obj

    def check_event_continue(self, value):
        query = {
            "event_name": value["event_name"],
            "country_code": value["country_code"],
            "start_date": {"$ne": value["start_date"]}
        }

        result = self.db.col_events().find_one(query)

        if result:
            _id = result.get("_id")
            update_query = {"$set": {"end_date": value["end_date"], "end_month": value["end_month"]}}
            self.db.col_events().update_one({"_id": _id}, update_query)
            return True
        return False
    def get_holidays(self):
        soup = BeautifulSoup(self.get_page(self.url), 'html.parser')
        table = soup.find(attrs={"class": "eventList"})
        rows = table.find_all('tr')[1:]
        events = []

        for row in rows:
            title = row.select_one(".eventName").text.strip()
            date = row.select_one(".eventDate").text.replace("\n", " ").strip()
            city = row.select_one(".eventCity").text.split(",")[0].strip()
            sub_categories = row.select_one(".eventCategory").text.split("/")
            date = self.convert_date(date)
            all_str = f"{date.strftime('%Y-%m-%d')} {title} {self.country_code}".lower().replace(
                " ", "_")
            instance = {
                    "event_year": date.year,
                    "start_month": date.month,
                    "end_month": date.month,
                    "country_code": self.country_code,
                    "start_date": int(date.strftime('%Y%m%d')),
                    "end_date": int(date.strftime('%Y%m%d')),
                    "dow": date.strftime('%A'),
                    "all_str": all_str,
                    "event_idx": self.generate_64_char_uuid(),
                    "event_name": title,
                    "categories": [
                        {"category": "concerts", "sub_categories": sub_categories}
                    ],
                    "city": city or None
                }

            stat = self.check_event_continue(instance)
            if stat:
                continue
            events.append(
                instance
            )

        return events


if __name__ == '__main__':
    with open("countries.json", "r") as json_file:
        data = json.load(json_file)

        for item in data:
            event = ConcertEvent(country=item)
            event.save_events()
