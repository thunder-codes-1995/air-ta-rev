from collections import OrderedDict
from typing import Any, Dict, Iterable, List, Optional, Tuple

from base.helpers.duration import Duration
from base.repository import BaseRepository


class ConfigurationRepository(BaseRepository):
    collection = "configuration"

    def get_by_key(self, key: str, host: str) -> Any:
        """get config value by key and host"""
        pipline = [
            {"$match": {"customer": {"$in": [host, "DEFAULT"]}}},
            {"$unwind": {"path": "$configurationEntries"}},
            {"$match": {"configurationEntries.key": {"$eq": f"{key}"}}},
            {"$project": {"_id": 0, "customer": "$customer", "value": "$configurationEntries.value"}},
        ]

        result = list(self.aggregate(pipline))
        filtered = list(filter(lambda obj: obj["customer"] != "DEFAULT", result))

        if filtered:
            return filtered[0]["value"]
        if result:
            return result[0]["value"]
        return

    def get_hosts(self):
        """get a list of all posiable unique hosts"""
        pipeline = [
            {"$unwind": {"path": "$configurationEntries"}},
            {"$match": {"configurationEntries.key": {"$eq": "COMPETITORS"}}},
            {"$project": {"configurationEntries.value": 1}},
            {"$unwind": {"path": "$configurationEntries.value"}},
            {"$project": {"_id": 0, "hosts": "$configurationEntries.value.competitors"}},
        ]

        hosts = []
        cursor = self.aggregate(pipeline)

        for item in cursor:
            for host in item["hosts"]:
                hosts.append(host)

        return list(set(hosts))

    def get_pos(self):
        pipeline = [
            {"$unwind": {"path": "$configurationEntries"}},
            {"$match": {"configurationEntries.key": "FILTER_POS"}},
            {"$project": {"_id": 0, "pos": "$configurationEntries.value"}},
        ]

        pos = []
        cursor = self.aggregate(pipeline)
        for item in cursor:
            for _pos in item["pos"]:
                pos.append(_pos)

        return list(set(pos))

    def get_currencies(self):
        pipeline = [
            {"$unwind": {"path": "$configurationEntries"}},
            {"$match": {"configurationEntries.key": "FILTER_CURRENCIES"}},
            {"$project": {"_id": 0, "currencies": "$configurationEntries.value"}},
        ]

        currencies = []
        cursor = self.aggregate(pipeline)
        for item in cursor:
            for currency in item["currencies"]:
                currencies.append(currency)

        return list(set(currencies))

    def update_market_cometitors(self, market: str, competitors: List[str], host: str):
        origin, destination = market.split("-")
        _competitors = self.get_by_key("COMPETITORS", host).get("value")

        for competitor in _competitors:
            if competitor["origin"] == origin and competitor["destination"] == destination:
                competitor["competitors"] = list(set(competitors))

        configs = self.get_configs_by_host(host)
        for config in configs:
            if config["key"] == "COMPETITORS":
                config["value"] = _competitors

        self.update_one({"customer": host}, {"configurationEntries": configs})

    def update_market_currencies(self, market: str, currency: str, host: str):
        origin, destination = market.split("-")
        markets = self.get_by_key("MARKETS", host).get("value")

        for market in markets:
            if market["orig"] == origin and market["dest"] == destination:
                market["value"] = currency

        configs = self.get_configs_by_host(host)
        for config in configs:
            if config["key"] == "MARKETS":
                config["value"] = markets

        self.update_one({"customer": host}, {"configurationEntries": configs})

    def get_configs_by_host(self, host: str):
        configs = self.find_one({"customer": host}).get("configurationEntries")
        return configs

    def update(self, data, host: str):
        configs = self.get_configs_by_host(host)
        for config in configs:
            if config["key"] in data:
                config["value"] = data[config["key"]]
        self.update_one({"customer": host}, {"configurationEntries": configs})

    def get_market_currency(self, host: str, market: str, default: str = None):
        """get default currency for a market (based on origin currency)"""
        configs = self.get_configs_by_host(host)
        cache_key = f"market_currency_{market}"
        if self.redis.get(cache_key):
            return self.redis.get(cache_key)

        market_currencies = list(filter(lambda rec: rec["key"] == "MARKETS", configs))[0]
        origin, destination = market.split("-")
        currencies = list(filter(lambda rec: rec["orig"] == origin and rec["dest"] == destination, market_currencies["value"]))

        if len(currencies):
            curr = currencies[0]["currency"]
            self.redis.set(cache_key, curr, expiration_in_seconds=Duration.months(1))
            return curr
        return default if default else None

    def get_user_default_settings(self, host: str):
        configs = self.get_configs_by_host(host)
        settings = list(filter(lambda obj: obj["key"] == "DEFAULT_USER_PROFILE", configs))
        settings = settings[0] if settings else None
        return settings

    def get_customers(self) -> Dict[str, List[str]]:
        """List of all customers"""
        customers: List[str] = self._db[self.collection].distinct("customer")
        customers.remove("DEFAULT")
        result = {"customers": customers}

        return result

    def get_market(self, customer: str, orig: str, dest: str):
        """Get the details of a market"""
        pipeline = [
            {"$match": {"customer": customer}},
            {"$unwind": {"path": "$configurationEntries"}},
            {"$match": {"configurationEntries.key": "MARKETS"}},
            {"$unwind": {"path": "$configurationEntries.value"}},
            {
                "$match": {
                    "$and": [
                        {"configurationEntries.value.dest": dest},
                        {"configurationEntries.value.orig": orig},
                    ]
                }
            },
            {
                "$addFields": {
                    "market": "$configurationEntries.value",
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "market.schedule.effectiveFrom": 0,
                    "market.schedule.effectiveTo": 0,
                    "market.schedule.frequency": 0,
                    "configurationEntries": 0,
                }
            },
        ]

        result = list(self.aggregate(pipeline))[0]
        return {**result["market"]}

    def get_customer_markets(self, customer: str) -> List[Dict[str, str]]:
        """Get the list of markets for a given customer"""
        pipeline = [
            {"$match": {"customer": customer}},
            {"$unwind": {"path": "$configurationEntries"}},
            {"$match": {"configurationEntries.key": "MARKETS"}},
            {"$unwind": {"path": "$configurationEntries.value"}},
            {
                "$project": {
                    "_id": 0,
                    "orig": "$configurationEntries.value.orig",
                    "dest": "$configurationEntries.value.dest",
                    "direction": "$configurationEntries.value.direction",
                    "currency": "$configurationEntries.value.currency",
                }
            },
        ]

        markets = list(self.aggregate(pipeline))
        return {"markets": markets}

    def check_customer_market_exists(self, customer: str, orig: str, dest: str) -> bool:
        """Check if a given market exists"""
        coll = self._db[self.collection]
        doc = coll.find_one(
            {
                "customer": customer,
                "configurationEntries": {"$elemMatch": {"key": "MARKETS", "value": {"$elemMatch": {"orig": orig, "dest": dest}}}},
            }
        )
        return bool(doc)

    def get_customer_comps(self, customer: str) -> Dict[str, List[str]]:
        """Get competitors for a given customer"""
        pipeline = [
            {"$match": {"customer": customer}},
            {"$unwind": {"path": "$configurationEntries"}},
            {"$match": {"configurationEntries.key": "COMPETITORS"}},
            {"$unwind": {"path": "$configurationEntries.value"}},
            {"$project": {"_id": 0, "competitors": "$configurationEntries.value.competitors"}},
        ]

        competitors: List[Dict[str, List[str]]] = list(self.aggregate(pipeline))
        result: Dict[str, List[str]] = competitors[0]

        return result

    def add_market_to_customer(self, customer: str, market: OrderedDict) -> None:
        coll = self._db[self.collection]
        coll.update_one(
            {"customer": customer, "configurationEntries.key": "MARKETS"}, {"$push": {"configurationEntries.$.value": market}}
        )

    def delete_market_from_customer(self, customer: str, orig: str, dest: str):
        coll = self._db[self.collection]
        coll.update_one(
            {"customer": customer, "configurationEntries.key": "MARKETS"},
            {"$pull": {"configurationEntries.$.value": {"orig": orig, "dest": dest}}},
        )

    def update_market(self, customer: str, orig: str, dest: str, market: OrderedDict) -> None:
        coll = self._db[self.collection]
        coll.update_one(
            {"customer": customer, "configurationEntries.key": "MARKETS"},
            {"$set": {"configurationEntries.$.value.$[market]": market}},
            array_filters=[{"market.orig": orig, "market.dest": dest}],
        )

    def get_supported_markets(self, customers: Optional[List[str]] = None, markets: Optional[Iterable[Tuple[str, str]]] = None):
        market_match = []
        if markets:
            for market in markets:
                market_match.append({"$and": [{"origin": market[0]}, {"destination": market[1]}]})

        pipline = [
            {"$match": {"customer": {"$in": customers}} if customers else {}},
            {"$unwind": {"path": "$configurationEntries"}},
            {"$match": {"configurationEntries.key": "MARKETS"}},
            {"$project": {"_id": 0, "customer": 1, "value": "$configurationEntries.value"}},
            {"$unwind": {"path": "$value"}},
            {"$project": {"origin": "$value.orig", "destination": "$value.dest", "customer": 1}},
            {"$match": {"origin": {"$in": [item[0] for item in markets]}} if markets else {}},
            {"$match": {"$or": market_match} if markets else {}},
        ]

        return self.aggregate(pipline)

    def get_supported_currencies(self) -> List[str]:
        """get list of unique supported currencies for all markets"""
        return [
            "EUR",
            "USD",
            "SAR",
            "AED",
            "GBP",
            "THB",
            "JPY",
            "BGN",
            "CZK",
            "DKK",
            "HUF",
            "PLN",
            "RON",
            "SEK",
            "CHF",
            "ISK",
            "NOK",
            "HRK",
            "RUB",
            "TRY",
            "AUD",
            "BRL",
            "CAD",
            "CNY",
            "HKD",
            "IDR",
            "ILS",
            "INR",
            "KRW",
            "MXN",
            "MYR",
            "NZD",
            "PHP",
            "SGD",
            "ZAR",
        ]
