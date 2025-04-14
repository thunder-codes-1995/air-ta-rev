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

class TradeShowsEvent(EventBase):

    def __init__(self, country):
        super().__init__(country)

        self.url = f"https://www.eventseye.com/fairs/{country['href']}"

    def get_date(self, date_element):
        date = date_element.split("<br/>")[0][4:]
        until = re.search(r'\d+', date_element.split("<br/>")[1].strip()).group()

        start_date = datetime.strptime(date, "%m/%d/%Y")
        end_date = start_date + timedelta(days=int(until))

        return start_date, end_date

    def get_holidays(self):
        soup = BeautifulSoup(self.get_page(self.url), 'html.parser')
        table = soup.select_one('table.tradeshows')
        events = []
        for row in table.select('tbody tr'):
            title = row.select_one('b:nth-child(1)').text
            try:
                start_date, end_date = self.get_date(
                    str(row.select_one('td:nth-child(4)')))
            except:
                continue
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
                        {"category": "trade_shows", "sub_categories": []}
                    ],
                    "city": None
                }
            )

        return events



if __name__ == '__main__':
    with open("countries.json", "r") as json_file:
        data = json.load(json_file)
        for item in data:
            event = TradeShowsEvent(country=item)
            event.save_events()
