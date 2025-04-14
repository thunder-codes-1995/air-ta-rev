import json
import os
import sys
from datetime import datetime

from bs4 import BeautifulSoup
project_root = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(project_root, '..', '..'))
sys.path.append(project_root)
from events import EventBase

class NationalEvent(EventBase):

    def __init__(self, country):
        super().__init__(country)
        self.current_year = datetime.now().year
        self.url = f"https://www.calendarlabs.com{country['href']}{self.current_year}"


    def serialize_holidays(self, holidays):

        formatted_data = {}

        for holiday, dates in holidays.items():
            if len(dates) == 1:
                date_obj = datetime.strptime(dates[0], "%b %d").replace(year=self.current_year)
                start_date = end_date = {
                    'int': int(date_obj.strftime("%Y%m%d")),
                    'month': int(date_obj.strftime("%m")),
                    'day': date_obj.strftime("%A"),
                }
            else:
                date_obj = datetime.strptime(dates[0], "%b %d").replace(year=self.current_year)
                end_date_obj = datetime.strptime(dates[-1], "%b %d")

                start_date = {
                    'int': int(date_obj.strftime("%Y%m%d")),
                    'month': int(date_obj.strftime("%m")),
                    'day': date_obj.strftime("%A"),
                }

                end_date = {
                    'int': int(end_date_obj.replace(year=self.current_year).strftime("%Y%m%d")),
                    'month': int(end_date_obj.strftime("%m")),
                }

            formatted_data[holiday] = {'start_date': start_date, 'end_date': end_date, 'date_obj': date_obj, 'title': holiday}

        return formatted_data

    def group_holidays(self, holidays):
        groups = {}
        for idx, value in enumerate(holidays.keys()):
            if idx == 0:
                continue
            second = value.split()
            first = list(holidays.keys())[idx-1].split()
            # Set can also be used to find common elements, but since indexing is lost, this method was used
            common = [element for element in second if element in first]

            if len(common) >= 2:

                index1 = [first.index(word) for word in common[:2]]
                index2 = [second.index(word) for word in common[:2]]
                if index1 == index2:
                    title = " ".join(common)
                    groups[title]['end_date'] = holidays[" ".join(second)]['end_date']

            else:
                groups[value] = holidays[value]

        return groups
    def get_holidays(self):
        soup = BeautifulSoup(self.get_page(self.url), 'html.parser')
        holidays = {}
        table = soup.select_one('table.hlist_tab')

        for row in table.select('tbody tr'):
            date = row.select_one('td:nth-child(2) span.mobile_text').text
            title = row.select_one('td:nth-child(3) a').text

            if holidays.get(title):
                holidays[title].append(date)
            else:
                holidays[title] = [date]

        formatted_holidays = self.serialize_holidays(holidays)
        events = []
        try:
            holidays = self.group_holidays(formatted_holidays)
        except:
            holidays = formatted_holidays

        for key, value in holidays.items():
            all_str = f"{value['date_obj'].strftime('%Y-%m-%d')} {key} {self.country_code}".lower().replace(
                " ", "_")
            events.append(
                {
                    "event_year": self.current_year,
                    "start_month": value["start_date"]["month"],
                    "end_month": value["end_date"]["month"],
                    "country_code": self.country_code,
                    "start_date": value["start_date"]["int"],
                    "end_date": value["end_date"]["int"],
                    "dow": value["start_date"]["day"],
                    "all_str": all_str,
                    "event_idx": self.generate_64_char_uuid(),
                    "event_name": value["title"],
                    "categories": [
                        {"category": "national", "sub_categories": []}
                    ],
                    "city": None
                }
            )
        return events



if __name__ == '__main__':
    with open("countries.json", "r") as json_file:
        data = json.load(json_file)
        for item in data:
            event = NationalEvent(country=item)
            event.save_events()
