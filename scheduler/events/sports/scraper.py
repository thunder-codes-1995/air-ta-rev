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

class SportsEvent(EventBase):

    def __init__(self, country):
        super().__init__(country)
        self.url = f"https://allsportdb.com{country['href']}"

    def parse_date(self, date_str):
        return datetime.strptime(date_str, "%d %B %Y").strftime('%Y%m%d')

    def get_holidays(self):
        soup = BeautifulSoup(self.get_page(self.url), 'html.parser')
        table = soup.find(attrs={'id': 'eventsTable'})
        events = []

        for row in table.find_all('tr')[1:]:
            title = row.select_one('.text-success').text
            dates = row.select('.align-middle.text-center.text-nowrap')[0].text.strip()
            sub_category = row.select_one('.text-warning').text.lower()
            city = None
            try:

                cities = row.select(".align-middle")[5].text.strip().replace("\n", "").split("-")
                for c in cities:
                    res = self.db.col_cities().find_one({"country_code": self.country_code, "city_name": c})
                    if res:
                        city = res["city_name"]
            except:
                pass

            match = re.match(r'(\d+)\s*([a-zA-Z]+)\s*-\s*(\d+)\s*([a-zA-Z]+)\s*(\d+)',
                             dates)
            match2 = re.match(r'(\d+)\s*-\s*(\d+)\s*([a-zA-Z]+)\s*(\d+)', dates)

            match3 = re.match(r'(\d{1,2})\s*([a-zA-Z]+)\s*(\d{4})', dates)

            if match:
                start_day, start_month, end_day, end_month, year = match.groups()
            elif match2:
                start_day, end_day, month, year = match2.groups()
                start_month = end_month = month
            elif match3:
                start_day, start_month, year = match3.groups()
                end_day = start_day
                end_month = start_month

            try:
                start_date = self.parse_date(f"{start_day} {start_month} {year}")
                end_date = self.parse_date(f"{end_day} {end_month} {year}")
            except UnboundLocalError as e:
                continue

            start_date_str = datetime.strptime(f"{start_day} {start_month} {year}",
                                               '%d %B %Y').strftime('%Y-%m-%d')
            all_str = f"{start_date_str} {title} {self.country_code}".lower().replace(" ","_")
            events.append(
                {
                    "event_year": int(year),
                    "start_month": int(
                        datetime.strptime(start_month, '%B').strftime('%m')),
                    "end_month": int(datetime.strptime(end_month, '%B').strftime('%m')),
                    "country_code": self.country_code,
                    "start_date": int(start_date),
                    "end_date": int(end_date),
                    "dow": datetime.strptime(start_date, '%Y%m%d').strftime('%A'),
                    "all_str": all_str,
                    "event_idx": self.generate_64_char_uuid(),
                    "event_name": title,
                    "categories": [
                        {"category": "sport", "sub_categories": [sub_category]}
                    ],
                    "city": city
                }
            )
        return events



if __name__ == '__main__':
    with open("countries.json", "r") as json_file:
        data = json.load(json_file)
        for item in data:
            event = SportsEvent(country=item)
            event.save_events()