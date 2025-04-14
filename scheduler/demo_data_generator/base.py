import calendar
import os
import sys


parentdir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parentdir)

from jobs.lib.utils.mongo_wrapper import MongoWrapper


class BaseDataGenerator:
    def __init__(self, carrier_code, origin, destination, year, month):
        self.db = MongoWrapper()
        self.carrier_code = carrier_code
        self.origin = origin
        self.destination = destination
        self.year = year
        self.month = month



    def get_days_range(self):
        days_in_month = calendar.monthrange(self.year, self.month)[1]

        days_range = range(1, days_in_month + 1)

        return days_range




