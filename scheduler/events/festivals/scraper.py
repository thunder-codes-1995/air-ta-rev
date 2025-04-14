import json
import os
import re
import string
import sys
from datetime import datetime
from bs4 import BeautifulSoup
project_root = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(project_root, '..', '..'))
sys.path.append(project_root)
from events import EventBase

class FestivalEvent(EventBase):

    def __init__(self, country):
        super().__init__(country)
        self.url = f"https://www.everfest.com{country['href']}"

    def parse_date(self, date_string):
        # 1. Possibility: Jun13-16,2024
        pattern1 = re.compile(r'(\w{3})(\d{1,2})-(\d{1,2}),(\d{4})')

        # 2. Possibility: Jun13,2024
        pattern2 = re.compile(r'(\w{3})(\d{1,2}),(\d{4})')

        # 3. Possibility: Feb3-Mar31,2024
        pattern3 = re.compile(r'(\w{3})(\d{1,2})-(\w{3})(\d{1,2}),(\d{4})')

        match1 = pattern1.match(date_string)
        match2 = pattern2.match(date_string)
        match3 = pattern3.match(date_string)

        if match1:
            month, day1, day2, year = match1.groups()
            date_start = datetime.strptime(f'{month}{day1}{year}', '%b%d%Y')
            date_end = datetime.strptime(f'{month}{day2}{year}', '%b%d%Y')
            return date_start, date_end

        elif match2:
            month, day, year = match2.groups()
            date = datetime.strptime(f'{month}{day}{year}', '%b%d%Y')
            return date, date

        elif match3:
            month_start, day_start, month_end, day_end, year = match3.groups()
            date_start = datetime.strptime(f'{month_start}{day_start}{year}', '%b%d%Y')
            date_end = datetime.strptime(f'{month_end}{day_end}{year}', '%b%d%Y')
            return date_start, date_end

        else:
            raise ValueError("Invalid date format")

    def get_holidays(self):
        soup = BeautifulSoup(self.get_page(self.url), 'html.parser')
        if soup.text.find("Sorry, no festivals here.") != -1:
            return []

        events = []

        divs = soup.select(".festival-card")

        for div in divs:
            if div.text.find("Dates Unconfirmed") != -1:
                continue

            title = div.select_one(".festival-card__title").text
            date = div.select_one(".festival-card__date").text
            city = div.select_one(".festival-card__location").text.split(",")[0]
            start_date, end_date = self.parse_date(date.strip().replace(" ", ""))
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
                        {"category": "conferences", "sub_categories": []}
                    ],
                    "city": city or None
                }
            )

        return events



if __name__ == '__main__':
    with open("countries.json", "r") as json_file:
        data = json.load(json_file)

        for item in data:
            event = FestivalEvent(country=item)
            event.save_events()
