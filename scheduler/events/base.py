import urllib.request
import uuid

import pycountry
from pymongo import UpdateOne
from datetime import datetime
from jobs.lib.utils.mongo_wrapper import MongoWrapper


class EventBase:
    def __init__(self, country):
        self.country = country["text"]
        try:
            self.country_code = pycountry.countries.get(name=self.country).alpha_2
        except:
            try:
                self.country_code = pycountry.countries.get(alpha_2=self.country).alpha_2
            except:
                self.country_code = None
        self.db = MongoWrapper()
        self.customers_list = self.get_customers_code()

    def get_customers_code(self):
        cursor = self.db.col_msd_config().find({"customer" : {"$ne": "DEFAULT"}})
        customers_code = []
        for document in cursor:
            customer_value = document.get("customer")
            customers_code.append(customer_value)
        return customers_code

    def get_page(self, url):
        headers = {
            'user-agent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36"}
        with urllib.request.urlopen(
                urllib.request.Request(url, headers=headers)) as response:
            return response.read()

    def generate_64_char_uuid(self):
        uuid_part1 = str(uuid.uuid4())
        uuid_part2 = str(uuid.uuid4())
        return (uuid_part1 + uuid_part2).replace("-", "")

    def save_events(self):
        if self.country_code is None:
            return
        try:
            events = self.get_holidays()
            if isinstance(events, list) and len(events) > 0:
                bulk_operations = []
                for event in events:
                    for customer in self.customers_list:
                        event_copy = event.copy()
                        event_copy.update({"airline_code": customer})
                        event_copy.update({"group_id": None})
                        event_copy.update({"lastUpdateDateTime": int(datetime.now().strftime("%Y%m%d%H%M"))})

                        try:
                            del event_copy["categories"][0]["sub_categories"][1]
                        except:
                            pass

                        res = self.db.col_cities().find_one(
                            {"country_code": self.country_code, "city_name": event_copy["city"]})

                        if res is None:
                            event_copy["city"] = None

                        filter_criteria = {
                            '$and': [
                                {'all_str': event_copy["all_str"]},
                                {'airline_code': event_copy["airline_code"]}
                            ]
                        }
                        update_data = {'$set': event_copy}
                        bulk_operations.append(UpdateOne(filter=filter_criteria,
                                                                 update=update_data,
                                                                 upsert=True))

                if bulk_operations:
                    self.db.col_events().bulk_write(bulk_operations)

        except Exception as e:
            return None