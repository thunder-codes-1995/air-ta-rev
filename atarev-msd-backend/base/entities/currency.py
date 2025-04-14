from dataclasses import dataclass
from typing import List, Union

from base.repository import BaseRepository


class CurrencyRepository(BaseRepository):
    collection = "currencies"

    def get_symbol(self, currency_code: str) -> str:
        cached = self.redis.get(f"currency_{currency_code}")

        if cached:
            return cached
        obj = self.find_one({"currency_code": currency_code, "symbol": {"$exists": True}})
        if not obj:
            return currency_code
        symbol = obj.get("symbol", currency_code)
        self.redis.set(f"currency_{currency_code}", symbol)
        return symbol

    def get_symbols(self, codes: List[str]):
        res = self.find({"currency_code": {"$in": codes}, "symbol": {"$exists": True}})
        m = {obj["currency_code"]: obj.get("symbol", obj["currency_code"]) for obj in res}
        return {code: m.get(code, code) for code in codes}


repo = CurrencyRepository()


@dataclass
class Currency:
    currency_code: Union[str, List[str]]

    @property
    def symbol(self):
        if type(self.currency_code) is list:
            return repo.get_symbols(self.currency_code)
        return repo.get_symbol(self.currency_code)

    @classmethod
    def attach_currency(self, value: Union[int, float, str], currency: str) -> str:
        return f"{currency} {value}"
