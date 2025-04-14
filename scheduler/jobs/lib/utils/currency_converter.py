import os
from jobs.lib.utils.mongo_wrapper import MongoWrapper
from jobs.lib.utils.redis_cache_client import RedisCacheClient


class CurrencyConverter:
    cache = RedisCacheClient()
    rates = None
    db = MongoWrapper()
    exchange_rates = None
    base_currency = "USD"
    country_codes_map = None

    def __init__(self, host: str = os.getenv("HOST")):
        self.host = host

    def get_currency_exchange_rates(self):
        if self.exchange_rates is None:
            self.exchange_rates = self.db.col_exchange_rates().find().sort("date", -1)[0]["rates"]
        return self.exchange_rates


    def get_market_currency(self, origin, destination):
        market_currency_map = self.cache.get(f"market_currency_map_{self.host}_{origin}_{destination}")
        if market_currency_map is None:
            market_currency_map = self.__get_currency_map()
            self.cache.set(f"market_currency_map_{self.host}_{origin}_{destination}", market_currency_map, 60 * 62)
        origin_country = self.__get_country_code(origin)
        destination_country = self.__get_country_code(destination)

        market_currency = None
        for item in market_currency_map:
            if item["origin"] == origin_country:
                if item["destination"] == destination_country:
                    market_currency = item["currency"]
                    break
                market_currency = item["currency"]

        return market_currency or self.base_currency

    def __get_currency_map(self):
        pipeline = [
            {"$match": {"customer": self.host}},
            {"$unwind": {"path": "$configurationEntries"}},
            {"$match": {"configurationEntries.key": "LFA_CALENDAR_CURRENCY"}},
            {"$project": {"curr": "$configurationEntries.value"}},
            {"$unwind": {"path": "$curr"}},
            {
                "$project": {
                    "_id": 0,
                    "originCountry": "$curr.originCountry",
                    "destinationCountry": "$curr.destinationCountry",
                    "currencyCode": "$curr.currencyCode",
                }
            },
        ]
        cursor = self.db.col_msd_config().aggregate(pipeline)
        mp = [
            {
                "origin": item.get("originCountry"),
                "destination": item.get("destinationCountry"),
                "currency": item.get("currencyCode"),
            }
            for item in cursor
        ]
        return mp

    def __get_country_code(self, iata_code):
        country_codes_map = self.cache.get(f"{iata_code}_country_code_map")
        if not country_codes_map:
            if not self.db:
                self.db = MongoWrapper()
            pipeline = [
                {"$match": {"airport_iata_code": iata_code}},
                {"$project": {"_id": 0, "airport_iata_code": "$airport_iata_code", "country_code": "$country_code"}}
            ]
            cursor = self.db.col_airports().aggregate(pipeline)
            country_codes_map = {item["airport_iata_code"]: item["country_code"] for item in cursor}

            if not country_codes_map:
                pipeline = [
                    {"$match": {"city_code": iata_code}},
                    {"$project": {"_id": 0, "city_code": "$city_code", "country_code": "$country_code"}}
                ]
                cursor = self.db.col_airports().aggregate(pipeline)
                country_codes_map = {item["city_code"]: item["country_code"] for item in cursor}
            self.cache.set(f"{iata_code}_country_code_map", country_codes_map, 60 * 61 * 6)

        return country_codes_map.get(iata_code)

    def get_default_currency(self):
        """get base currency for a host"""
        default_currency = self.cache.get(f"{self.host}_default_currency")
        if not default_currency:
            if not self.db:
                self.db = MongoWrapper()
            pipeline = [
                {"$match": {"customer": self.host}},
                {"$unwind": {"path": "$configurationEntries"}},
                {"$match": {"configurationEntries.key": "DEFAULT_CURRENCY"}},
                {"$project": {"_id": 0, "default_currency": "$configurationEntries.value"}},
            ]
            default_currency = list(self.db.col_msd_config().aggregate(pipeline))[0]["default_currency"]
            self.cache.set(f"{self.host}_default_currency", default_currency, 60 * 61)
        return default_currency

    def convert_currency(self, from_currency, to_currency, amount):
        """convert fare currency (based on USD)"""
        if from_currency == to_currency:
            return amount
        if self.rates is None:
            self.rates = self.get_currency_exchange_rates()
        usd_based_ratio = self.rates.get("USD") / self.rates.get(from_currency, 1)
        usd_based_amount = amount * usd_based_ratio
        ratio = self.rates.get(to_currency, 1) / self.rates.get("USD")
        return usd_based_amount * ratio
