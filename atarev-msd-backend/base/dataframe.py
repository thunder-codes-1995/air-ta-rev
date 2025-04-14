import pandas as pd

from base.entities.exchange_rate import ExchangeRate
from utils.funcs import from_int_to_datetime, get_date_as_string


class DataFrame(pd.DataFrame):
    def add_date(self, year_col, month_col, day_col=None):
        if not day_col:
            self["date"] = pd.to_datetime(self[year_col] * 10000 + self[month_col] * 100 + 1, format="%Y%m%d")
        else:
            self["date"] = pd.to_datetime(
                self[year_col] * 10000 + self[month_col] * 100 + self[day_col],
                format="%Y%m%d",
            )

    def date_as_int(self, target_col: str, col: str = "date"):
        self[col] = self[target_col].apply(from_int_to_datetime)

    def date_as_str(self, target_col: str, col: str = "date"):
        self[col] = self[target_col].apply(get_date_as_string)

    def date_from_int(self, target_col: str, col: str = "date"):
        self[col] = pd.to_datetime(self[target_col]).dt.date

    def convert_currency(self, value_col: str, currency_col: str = None, convert_to: str = None, base_currency="USD"):
        rate = ExchangeRate(base_currency, [convert_to or "USD"]).get_single_base_rates(base_currency)[convert_to or "USD"]
        self[value_col] = self[value_col] * rate
        if currency_col:
            self[currency_col] = self.shape[0] * [convert_to or "USD"]
        else:
            self["currency"] = self.shape[0] * [convert_to or "USD"]

    def unique_as_string(self, col_name: str, sep=","):
        return sep.join(self[col_name].unique().tolist())
