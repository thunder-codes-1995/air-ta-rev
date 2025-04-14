import sys
from datetime import datetime
from pymongo import InsertOne

from demo_data_generator.base import BaseDataGenerator


class DdsPgsGenerator(BaseDataGenerator):

    def get_instances(self):
        query = {
            "travel_date": {
                "$gte": 20230000,
                "$lt": 20240000
            },
            "dom_op_al_code": self.carrier_code
        }

        return self.db.col_dds().find(query).limit(31)

    def prepare_document(self, data, day):
        date = f"{self.year}{self.month:02d}{day:02d}"

        date_object = datetime.strptime(date, '%Y%m%d')

        data['travel_date'] = int(date)
        outbound_day_of_week = date_object.weekday() + 1
        data['travel_day_of_week'] = outbound_day_of_week
        data["dest_code"] = self.destination
        data["orig_code"] = self.origin
        data["travel_month"] = self.month
        data["travel_year"] = self.year
        del data["_id"]
        return data

    def generate(self):
        data = list(self.get_instances())
        rng = self.get_days_range()
        bulk_operations = []
        for day in rng:
            new_data = self.prepare_document(data[day], day)
            bulk_operations.append(InsertOne(new_data))

        if bulk_operations:
            self.db.col_dds().bulk_write(bulk_operations)


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

    generator = DdsPgsGenerator("XX", "AUH", "DEL", 2024, 1)
    generator.generate()
