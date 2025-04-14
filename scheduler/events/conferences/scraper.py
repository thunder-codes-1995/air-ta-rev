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

class ConferenceEvent(EventBase):

    def __init__(self, country):
        super().__init__(country)
        self.url = country['href']

    def convert_date(self, date_str):
        date_obj = datetime.strptime(date_str, '%m-%d-%Y')
        return date_obj

    def get_holidays(self):
        soup = BeautifulSoup(self.get_page(self.url), 'html.parser')

        if soup.text.find("No Conference") != -1:
            return []

        divs = soup.find_all(attrs={"class" : "item"})
        events = []

        for div in divs:
            title = div.select_one("h3").text
            sub_category = div.find('b', text='Category:').find_next_sibling(text=True).strip()
            start_date = div.find('b', text='Start Date:').find_next_sibling(
                text=True).strip()
            last_day = div.find('b', text='Last Day:').find_next_sibling(
                text=True).strip()

            start_date = self.convert_date(start_date)
            end_date = self.convert_date(last_day)

            all_str = f"{start_date.strftime('%Y-%m-%d')} {title} {self.country_code}".lower().replace(
                " ", "_")

            events.append(
                {
                    "event_year": start_date.year,
                    "start_month": start_date.month,
                    "end_month": end_date.month,
                    "country_code": self.country_code,
                    "start_date": int(start_date.strftime('%Y%m%d')),
                    "end_date": int(end_date.strftime('%Y%m%d')),
                    "dow": start_date.strftime('%A'),
                    "all_str": all_str,
                    "event_idx": self.generate_64_char_uuid(),
                    "event_name": title,
                    "categories": [
                        {"category": "conferences", "sub_categories": [sub_category]}
                    ],
                    "city": None
                }
            )

        return events



if __name__ == '__main__':
    with open("countries.json", "r") as json_file:
        data = json.load(json_file)

        for item in data:
            event = ConferenceEvent(country=item)
            event.save_events()
