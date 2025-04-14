from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, TypedDict, Union

import pymongo
from dotenv import load_dotenv

from base.repository import BaseRepository

load_dotenv()


class ExchangeRate(TypedDict):
    date: int
    rates: Dict[str, float]


class ExchangeRepo(BaseRepository):
    collection = "exchange_rates"


exchange_repo = ExchangeRepo()


@dataclass
class HistoricalExchangeRate:
    base_currency: Union[str, List[str]]
    currencies: List[str]

    def history(self, start_date: Optional[int] = None, end_date: Optional[int] = None) -> Iterable[ExchangeRate]:
        _and = []

        if start_date:
            _and.append({"date": {"$gte": int(f"{start_date}000000")}})
        if start_date:
            _and.append({"date": {"$lte": int(f"{end_date}000000")}})

        stages = [
            {"$match": {"$and": _and} if _and else {}},
            {"$addFields": {"dt": {"$toString": "$date"}}},
            {"$project": {"_id": 0, "rates": 1, "dt": {"$toInt": {"$substr": ["$dt", 0, 8]}}}},
            {"$sort": {"dt": -1}},
            {"$project": {"date": "$dt", "dt": -1, "rates": 1}},
        ]

        c = exchange_repo.aggregate(stages)
        return c

    def rates(self, start_date: Optional[int] = None, end_date: Optional[int] = None) -> Dict[int, Dict[str, float]]:
        res: Dict[int, Dict[str, float]] = {}
        cursor = self.history(start_date, end_date)

        if type(self.base_currency) is str:
            for r in cursor:
                res[r["date"]] = {}
                for curr in self.currencies:
                    conversion_rate = r["rates"][curr] / r["rates"][self.base_currency]
                    res[r["date"]].update({f"{self.base_currency}-{curr}": conversion_rate})

            return res

        for r in cursor:
            res[r["date"]] = {}
            for base in self.base_currency:
                for curr in self.currencies:
                    conversion_rate = r["rates"][curr] / r["rates"][base]
                    res[r["date"]].update({f"{base}-{curr}": conversion_rate})
        return res


@dataclass
class ExchangeRate:
    base_currency: Union[str, List[str]]
    currencies: List[str]

    @property
    def latest_record(self) -> dict:
        latest_record = exchange_repo.find_one(sort=[("date", pymongo.DESCENDING)])
        return latest_record["rates"]

    def rates(self):
        if not self.currencies:
            return {}

        if type(self.base_currency) is str:
            return self.get_single_base_rates(self.base_currency)

        return self.get_multi_base_rate()

    def get_single_base_rates(self, base_currency: str) -> Dict[str, float]:
        results = {}
        for currency in self.currencies:
            results.update({currency: self.__convert_currency(base_currency, currency, self.latest_record)})
        return results

    def get_multi_base_rate(self) -> Dict[str, str]:
        rates = {}
        for base in self.base_currency:
            rates = {**rates, **self.get_single_base_rates(base)}
        return rates

    def __convert_currency(self, base_currency, target_currency, exchange_rates) -> float:
        # Check input parameters
        if base_currency not in exchange_rates or target_currency not in exchange_rates:
            raise ValueError("Invalid currency")
        # Calculate the conversion rate
        conversion_rate = exchange_rates[target_currency] / exchange_rates[base_currency]
        # Calculate and return
        return conversion_rate
