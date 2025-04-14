import json
import os
import re
import sys
import urllib.request
from datetime import datetime
from bs4 import BeautifulSoup

project_root = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(project_root, '..', '..'))
sys.path.append(project_root)

from events import EventBase

class PoliticalEvent(EventBase):

    def __init__(self, country):
        super().__init__(country)
        self.current_year = datetime.now().year
        self.event = country["event"]

    def parse_date(self, date_str):
        pattern_1 = r"(\d{1,2}) ([A-Za-z]{3}) (\d{4})"
        pattern_2 = r"(\d{1,2})-(\d{1,2}) ([A-Za-z]{3}) (\d{4})"
        pattern_3 = r"(\d{1,2}) ([A-Za-z]{3}) - (\d{1,2}) ([A-Za-z]{3}) (\d{4})"

        match_1 = re.match(pattern_1, date_str)
        match_2 = re.match(pattern_2, date_str)
        match_3 = re.match(pattern_3, date_str)

        if match_1:
            day, month_str, year = match_1.groups()
            month = datetime.strptime(month_str, '%b').month
            return datetime(int(year), month, int(day))
        elif match_2:
            start_day, end_day, month_str, year = match_2.groups()
            month = datetime.strptime(month_str, '%b').month
            return (datetime(int(year), month, int(start_day)),
                    datetime(int(year), month, int(end_day)))
        elif match_3:
            start_day, start_month_str, end_day, end_month_str, year = match_3.groups()
            start_month = datetime.strptime(start_month_str, '%b').month
            end_month = datetime.strptime(end_month_str, '%b').month
            return (datetime(int(year), start_month, int(start_day)),
                    datetime(int(year), end_month, int(end_day)))
        else:
            return None

    def get_holidays(self):
        events = []
        date = self.parse_date(self.event["date"])
        if date:
            if not isinstance(date, tuple):
                start_date = date
                end_date = date
            else:
                start_date = date[0]
                end_date = date[1]

            title = self.event["title"]
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
                        {"category": "political", "sub_categories": []}
                    ],
                    "city": None
                }
            )
        else:
            return None
        return events

def get_page(url):
    headers = {
        'user-agent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36"}
    with urllib.request.urlopen(
            urllib.request.Request(url, headers=headers)) as response:
        return response.read()

if __name__ == '__main__':
    url = "https://www.controlrisks.com/our-thinking/geopolitical-calendar"
    soup = BeautifulSoup(get_page(url), 'html.parser')
    events = []
    table = soup.select_one('table')
    for row in table.select('tbody tr'):
        try:
            events.append({
                "event": {
                    "date": row.select_one('td:nth-child(1)').text.strip(),
                    "title": row.select_one('td:nth-child(2)').text.strip(),
                },
                "country": row.select_one('td:nth-child(3)').text.strip()
            })
        except:
            print("Error")

    for event in events:
        alert_events = PoliticalEvent(country={"text": event["country"], "event": event["event"]})
        alert_events.save_events()

