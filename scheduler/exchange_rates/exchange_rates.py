import os
import sys
from datetime import datetime
import requests
from dotenv import load_dotenv

parentdir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parentdir)

from jobs.lib.utils.mongo_wrapper import MongoWrapper

load_dotenv()


class ExchangeRates:
    def __init__(self):
        key = os.getenv("EXCHANGE_RATE_KEY")
        self.url = f"https://api.currencybeacon.com/v1/latest?api_key={key}"
        self.db = MongoWrapper()

    def __get_rates(self):
        response = requests.get(url=self.url).json()
        rates = response["rates"]
        dt_object = datetime.strptime(response["date"], "%Y-%m-%dT%H:%M:%SZ")
        date = dt_object.strftime("%Y%m%d%H%M%S")
        return {
            "rates": rates,
            "date": int(date)
        }

    def add_rate(self):
        rates = self.__get_rates()
        self.db.col_exchange_rates().insert_one(rates)


if __name__ == '__main__':
    exchange_rates = ExchangeRates()
    exchange_rates.add_rate()
