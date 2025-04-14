from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Literal

import pandas as pd
from core.db import DB
from core.meta import read_meta

from .meta import DDsRESMeta


@dataclass
class DDsSource:
    date: int
    path: str

    def __post_init__(self):
        data = read_meta(
            meta_class=DDsRESMeta,
            date=self.date,
            path=self.path,
            override_header=True,
            sep=",",
            encoding="Windows-1252",
        )

        airports = data.origin.unique().tolist() + data.destination.unique().tolist()
        cities = data.city_pos.unique().tolist()

        airport_city_map = self.__airport_city_map(airports)
        city_country_map = self.__city_country_map(cities)

        data["is_group"] = data.pax.apply(self.__is_group)
        data["ticket_type"] = data.path.apply(self.__ticket_type)
        data["str_dept_date"] = data.dept_date.apply(self.__str_date)
        data["str_bkng_date"] = data.bkng_date.apply(self.__str_date)
        data["days_sold_prior_to_travel"] = data.apply(self.__days_sold_prior_to_travel, axis=1)
        data["local_dep_time"] = data.dept_datetime.apply(self.__dept_time)
        data["city_orig_code"] = data.origin.map(airport_city_map)
        data["city_dest_code"] = data.destination.map(airport_city_map)
        data["cos_norm"] = data.city_pos.map(city_country_map)
        data["country_of_sale"] = data.city_pos.map(city_country_map)
        data["travel_year"] = data.str_dept_date.str[0:4].astype(int)
        data["travel_month"] = data.str_dept_date.str[5:7].astype(int)
        data["sell_year"] = data.str_bkng_date.str[0:4].astype(int)
        data["sell_month"] = data.str_bkng_date.str[5:7].astype(int)
        data["avf_fare_curr"] = data.avg_fare.str[0:1]
        data["travel_day_of_week"] = data.str_dept_date.apply(self.__travel_day_of_week)
        data.dept_date = data.dept_date.apply(self.__int_date)
        data.bkng_date = data.bkng_date.apply(self.__int_date)

        currencies = data.avf_fare_curr.unique().tolist()
        currency_map = self.__currency_map(currencies)
        data.avf_fare_curr = data.avf_fare_curr.map(currency_map)
        data.avg_fare = data.avg_fare.str[1:].astype(float)
        data.aircraft_type = data.aircraft_type.astype(str)

        data.rename(
            columns={
                "dept_date": "travel_date",
                "cabin": "seg_class",
                "class": "rbkd",
                "origin": "orig_code",
                "destination": "dest_code",
                "op_airline_code": "dom_op_al_code",
                "bkng_date": "sell_date",
                "aircraft_type": "equip",
                "mk_flt_num": "op_flt_num",
            },
            inplace=True,
        )
        self.data = data[
            [
                "travel_date",
                "sell_date",
                "rbkd",
                "orig_code",
                "dest_code",
                "fare_basis",
                "op_flt_num",
                "dom_op_al_code",
                "is_group",
                "ticket_type",
                "days_sold_prior_to_travel",
                "local_dep_time",
                "city_orig_code",
                "city_dest_code",
                "seg_class",
                "equip",
                "pax",
                "avg_fare",
                "sell_year",
                "sell_month",
                "cos_norm",
                "country_of_sale",
                "avf_fare_curr",
                "travel_month",
                "travel_year",
                "travel_day_of_week",
            ]
        ]

    def __int_date(self, val: str) -> int:
        return int("".join(reversed(val.split("/"))))

    def __str_date(self, val: str) -> str:
        return "-".join(reversed(val.split("/")))

    def __ticket_type(self, val: str) -> Literal["OW", "RT"]:
        return "RT" if len(val.split("-")) > 2 else "OW"

    def __is_group(self, val: int) -> bool:
        return val >= 9

    def __days_sold_prior_to_travel(self, row: pd.Series) -> int:
        dept_date = datetime.strptime(row.str_dept_date, "%Y-%m-%d")
        bkng_date = datetime.strptime(row.str_bkng_date, "%Y-%m-%d")
        return (dept_date - bkng_date).days

    def __dept_time(self, value: str) -> int:
        _, time = value.split(" ")
        return int(time.replace(":", ""))

    def __airport_city_map(self, airport_list: List[str]) -> Dict[str, str]:
        c = DB().airports.find({"airport_iata_code": {"$in": airport_list}})
        return {item["airport_iata_code"]: item["city_code"] for item in c}

    def __city_country_map(self, city_list: List[str]) -> Dict[str, str]:
        c = DB().airports.find({"city_code": {"$in": city_list}})
        return {item["city_code"]: item["country_code"] for item in c}

    def __currency_map(self, currency_code: List[str]) -> Dict[str, str]:
        c = DB().currency.find({"symbol": {"$in": currency_code}})
        return {item["symbol"]: item["currency_code"] for item in c}

    def __travel_day_of_week(self, value: str) -> int:
        date = datetime.strptime(value, "%Y-%m-%d").date()
        return date.weekday() + 1
