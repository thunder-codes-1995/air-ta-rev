from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any, Dict, Optional, Tuple, Union

import pandas as pd
from flask import request

from regions.repository import RegionRepository

repo = RegionRepository()


@dataclass
class Grouped:
    host_code: Union[str, None] = field(init=False, default=None)
    periods: Union[Tuple, None] = field(init=False, default=None)

    def __post_init__(self):
        self.host_code = request.user.carrier
        self.periods = (
            (0, None, True, False),  # 1st column
            (8, None, True, True),  # 2nd and 3rd
            (8, self.host_code, True, True),  # 4th, 5th
            (0, self.host_code, True, False),
            (1, self.host_code, True, False),
            (2, self.host_code, True, False),
            (3, self.host_code, True, False),
            (4, self.host_code, True, False),
            (5, self.host_code, True, False),
            (6, self.host_code, True, False),
            (7, self.host_code, True, False),
        )

    def calc_period_stats(self, data: pd.DataFrame, calc_sum=True, calc_avg=True, prefix=""):
        pax_result: Dict[str, Any] = {}
        fare_result: Dict[str, Any] = {}

        if calc_sum:
            pax_result[f"{prefix}_sum"] = int(data.pax.sum())
            fare_result[f"{prefix}_sum"] = int(data.blended_fare.sum())

        if calc_avg:
            pax_result[f"{prefix}_avg"] = float(f"{data.pax.mean():.2f}") if pax_result.get("sum") else 0
            fare_result[f"{prefix}_avg"] = float(f"{data.blended_fare.mean():.2f}") if fare_result.get("sum") else 0

        return pax_result, fare_result

    def calc_stats(self):
        pax_result = {}
        fare_result = {}
        for week_order, host_code, calc_sum, calc_avg in self.periods:
            targeted = Week(week_order, self.data, host_code).get()
            prefix = f"{week_order}_{host_code}" if host_code else f"{week_order}"
            res = self.calc_period_stats(targeted, calc_sum, calc_avg, prefix)
            pax_result.update(res[0])
            fare_result.update(res[1])

        return pax_result, fare_result


@dataclass
class MarketGrouped(Grouped):
    market_code: str
    data: pd.DataFrame

    def calc(self):
        return self.calc_stats()


@dataclass
class CountryGrouped(Grouped):
    country_code: str
    data: pd.DataFrame

    def calc(self):
        pax_markets = []
        fare_markets = []
        for market, market_df in self.data.groupby("market"):
            res = MarketGrouped(market, market_df).calc()
            pax_markets.append({"market_code": market, **res[0]})
            fare_markets.append({"market_code": market, **res[1]})

        res = self.calc_stats()
        return {**res[0], "markets": pax_markets}, {**res[1], "markets": fare_markets}


@dataclass
class RegionGrouped(Grouped):
    region_code: str
    data: pd.DataFrame

    def calc(self):
        pax_countries = []
        fare_countries = []
        for country, country_df in self.data.groupby("country_of_sale"):
            res = CountryGrouped(country, country_df).calc()
            pax_countries.append({"country_code": country, **res[0]})
            fare_countries.append({"country_code": country, **res[1]})
        res = self.calc_stats()
        return {**res[0], "countries": pax_countries}, {**res[1], "markets": fare_countries}


@dataclass
class Week:
    count_in_past: int
    data: pd.DataFrame
    host_code: Optional[str] = None
    start_date: int = field(init=False)
    end_date: int = field(init=False)

    def __post_init__(self):
        self.start_date = date.today() - timedelta(self.count_in_past * 7)
        self.end_date = self.start_date - timedelta(days=7)

    def get(self) -> pd.DataFrame:
        start, end = int(self.end_date.strftime("%Y%m%d")), int(self.start_date.strftime("%Y%m%d"))
        targeted = self.data[(self.data.travel_date >= start) & (self.data.travel_date <= end)]

        if self.host_code:
            targeted = targeted[targeted.dom_op_al_code == self.host_code]

        return targeted


@dataclass
class Table:
    data: pd.DataFrame
    host_code: str

    def __post_init__(self):
        counties = self.data.country_of_sale.unique().tolist()
        cur = repo.get_countries_grouped_by_region(country_codes=counties)
        country_region_map = {item["country_code"]: item["region_code"] for item in cur}
        self.data["region"] = self.data.country_of_sale.map(country_region_map)
        self.data["market"] = self.data.origin + "-" + self.data.destination
        self.data.region.fillna("-", inplace=True)
        self.data.country_of_sale.fillna("-", inplace=True)

    def get(self):
        pax_result = []
        fare_result = []
        for region, region_df in self.data.groupby("region"):
            pax_row, fare_row = RegionGrouped(region, region_df).calc()
            pax_result.append({"region": region, **pax_row})
            fare_result.append({"region": region, **fare_row})

        return pax_result, fare_result

    @property
    def pax_columns(self):
        return {
            f"0_sum": "TW MKT PAX",
            f"8_sum": "8WK MKT PAX TRENDS",
            f"8_avg": "8WK AVG MKT PAX",
            f"8_{self.host_code}_sum": "8WK HOST PAX TRENDS",
            f"8_{self.host_code}_avg": "8WK AVG HOST PAX",
            f"0_{self.host_code}_sum": "TW HOST PAX",
            f"1_{self.host_code}_sum": "W-1 HOST PAX",
            f"2_{self.host_code}_sum": "W-2 HOST PAX",
            f"3_{self.host_code}_sum": "W-3 HOST PAX",
            f"4_{self.host_code}_sum": "W-4 HOST PAX",
            f"5_{self.host_code}_sum": "W-5 HOST PAX",
            f"6_{self.host_code}_sum": "W-6 HOST PAX",
            f"7_{self.host_code}_sum": "W-7 HOST PAX",
            "region": "REGION",
        }

    @property
    def fare_columns(self):
        return {
            f"0_sum": "TW MKT REV",
            f"8_sum": "8WK MKT REV TRENDS",
            f"8_avg": "8WK AVG MKT REV",
            f"8_{self.host_code}_sum": "8WK HOST REV TRENDS",
            f"8_{self.host_code}_avg": "8WK AVG HOST REV",
            f"0_{self.host_code}_sum": "TW HOST REV",
            f"1_{self.host_code}_sum": "W-1 HOST REV",
            f"2_{self.host_code}_sum": "W-2 HOST REV",
            f"3_{self.host_code}_sum": "W-3 HOST REV",
            f"4_{self.host_code}_sum": "W-4 HOST REV",
            f"5_{self.host_code}_sum": "W-5 HOST REV",
            f"6_{self.host_code}_sum": "W-6 HOST REV",
            f"7_{self.host_code}_sum": "W-7 HOST REV",
            "region": "REGION",
        }
