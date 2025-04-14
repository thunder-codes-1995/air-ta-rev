from collections import defaultdict

import requests
import xmltodict

import json
import os
import sys
from datetime import datetime

project_root = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(project_root, '..', '..'))
sys.path.append(project_root)
from events import EventBase

class AlertEvents(EventBase):

    def __init__(self, country):
        super().__init__(country)
        self.alerts = country["alerts"]

    def convert_date(self, date_str):
        return datetime.strptime(date_str, '%a, %d %b %Y %H:%M:%S %Z')

    def get_holidays(self):
        events = []

        for alert in self.alerts:
            start_date = self.convert_date(alert["gdacs:fromdate"])
            end_date = self.convert_date(alert["gdacs:todate"])
            title = alert["title"]
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
                        {"category": "alert", "sub_categories": []}
                    ],
                    "city": None
                }
            )
        return events


if __name__ == '__main__':
    url = 'https://gdacs.org/xml/rss.xml'

    response = requests.get(url)

    dict_data = xmltodict.parse(response.content)
    alerts = dict_data['rss']['channel']['item']
    grouped_data = defaultdict(list)

    for item in alerts:
        countries = item.get('gdacs:country')
        if countries:
            for country in countries.split(','):
                grouped_data[country.strip()].append(item)

    for country, alert_list in grouped_data.items():
        alert_events = AlertEvents(country={"text": country, "alerts": alert_list})
        alert_events.save_events()




