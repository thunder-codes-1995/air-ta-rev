import sys
from datetime import datetime
from pymongo import InsertOne

from demo_data_generator.base import BaseDataGenerator


class Fares2DataGenerator(BaseDataGenerator):

    def get_fares(self):
        query = {
            "outboundDate": {
                "$gte": 20230000,
                "$lt": 20240000
            },
            "carrierCode": self.carrier_code
        }

        return self.db.col_fares_processed().find(query)

    def prepare_document(self, data, day):
        date = f"{self.year}{self.month:02d}{day:02d}"

        date_object = datetime.strptime(date, '%Y%m%d')

        data['outboundDate'] = int(date)
        outbound_day_of_week = date_object.weekday()
        data['outboundDayOfWeek'] = outbound_day_of_week
        data["marketDestination"] = self.destination
        data["marketOrigin"] = self.origin
        del data["_id"]
        return data

    def generate(self):
        data = list(self.get_fares())
        rng = self.get_days_range()
        bulk_operations = []
        for day in rng:
            new_data = self.prepare_document(data[day], day)
            bulk_operations.append(InsertOne(new_data))

        if bulk_operations:
            self.db.col_fares_processed().bulk_write(bulk_operations)


if __name__ == '__main__':
    if len(sys.argv) != 6:
        print(
            "Missing Arguments\nUsage: python script.py carrier_code origin destination year month")
        sys.exit(1)

    carrier_code = sys.argv[1]
    origin = sys.argv[2]
    destination = sys.argv[3]
    year = sys.argv[4]
    month = sys.argv[5]

    try:
        year, month = int(year), int(month)
    except ValueError:
        print(
            "Wrong Format\nyear and month should be numbers")
        sys.exit(1)

    generator = Fares2DataGenerator(carrier_code, origin, destination, year, month)
    generator.generate()
