import json
import os
import sys
from datetime import datetime

from bs4 import BeautifulSoup
project_root = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(project_root, '..', '..'))
sys.path.append(project_root)
from events import EventBase

class SchoolEvents(EventBase):

    def __init__(self, country):
        super().__init__(country)
        self.url = f"{country['href']}school-holidays/"

    def convert_date(self, date_string):
        date = {
                "integer_value": None,
                "month": None,
                "year": None,
                "day_name": None,
                "date_object": None
            }
        if date_string is not None:
            date_string = date_string[:-5].strip()
            date_object = datetime.strptime(date_string, '%d %b %Y')

            date["integer_value"] = int(date_object.strftime('%Y%m%d'))
            date["month"] = date_object.month
            date["year"] = date_object.year
            date["day_name"] = date_object.strftime('%A')
            date["date_object"] = date_object

        return date



    def merge_territories(self, soup):
        rows = []
        table = soup.find(attrs={'class': 'tablepress'})
        if table is None:
            return None
        links = table.find_all("a")
        for link in links:
            soup2 = BeautifulSoup(self.get_page(link["href"]), 'html.parser')
            tmp_table = soup2.find(attrs={"class":"publicholidays"})
            inner_rows = []
            for row in tmp_table.find_all('tr')[1:]:
                row["city"] = link.text
                inner_rows.append(row)

            rows.extend(inner_rows)
        return rows

    def try_with_year(self):
        year_str = str(datetime.now().year)
        url = self.url + f"{year_str}-dates/"
        soup = BeautifulSoup(self.get_page(url), 'html.parser')
        table = soup.find(attrs={"class": "publicholidays"})
        if table is None:
            return None
        else:
            rows = table.find_all('tr')[1:]
            return rows

    def get_holidays(self):
        soup = BeautifulSoup(self.get_page(self.url), 'html.parser')

        table = soup.find(attrs={"class":"publicholidays"})

        if table is None:
            rows = self.merge_territories(soup)
            if rows is None:
                rows = self.try_with_year()
                if rows is None:
                    return None
        else:
            rows = table.find_all('tr')[1:]

        events = []
        for row in rows:
            title = row.select_one('.summary').text
            start_date = row.find_all('td')[1].text.strip()
            city = row.get("city")
            finish_date = row.find_all('td')[2].text.strip() or None

            start_date = self.convert_date(start_date)
            finish_date = self.convert_date(finish_date)

            all_str = f"{start_date['date_object'].strftime('%Y-%m-%d')} {title} {self.country_code}".lower().replace(
                " ", "_")

            events.append(
                {
                    "event_year": start_date["year"],
                    "start_month": start_date["month"],
                    "end_month": finish_date["month"] or start_date["month"],
                    "country_code": self.country_code,
                    "start_date": start_date["integer_value"],
                    "end_date": finish_date["integer_value"] or start_date["integer_value"],
                    "dow": start_date["day_name"],
                    "all_str": all_str,
                    "event_idx": self.generate_64_char_uuid(),
                    "event_name": title,
                    "categories": [
                        {"category": "school", "sub_categories": []}
                    ],
                    "city": city or None
                }
            )
        return events

    def is_exists(self, event):
        existing_document = self.db.col_events().find_one(
            {'all_str': event["all_str"], 'airline_code': event["airline_code"]})
        return existing_document is not None






if __name__ == '__main__':
    with open("countries.json", "r") as json_file:
        data = json.load(json_file)

        for item in data:
            school_events = SchoolEvents(country=item)
            school_events.save_events()
