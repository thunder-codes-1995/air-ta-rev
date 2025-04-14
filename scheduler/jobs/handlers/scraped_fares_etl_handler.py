from .base import FieldsHandler
from datetime import datetime


class ScraperFaresEtlHandler(FieldsHandler):

    def scrapeTime_handler(self, row, col):
        # sometimes i get scrapeTime as string and sometimes i get it as datetime object
        # i need to make sure it is always datetime
        return (datetime.strptime(row[col].split('.')[0], "%Y-%m-%d %H:%M:%S")
                if type(row[col]) == str else row[col])
