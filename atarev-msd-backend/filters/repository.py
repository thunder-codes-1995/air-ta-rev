from typing import Dict, Iterable, Optional, TypedDict

from base.helpers.duration import Duration
from base.repository import BaseRepository


class Market(TypedDict):
    orig: str
    dest: str


class FilterRepository(BaseRepository):
    collection = "filter_configuration"

    def get_market(self, orig: str, dest: str, host: str):
        cache_key = f"{orig}-{dest}_filters_market"
        market = self.redis.get(cache_key)
        if market:
            return market

        record = self.find_one({"customer": host})

        if not record:
            return

        market = list(
            filter(
                lambda item: (item["market_origin_city_code"] == orig and item["market_destination_city_code"] == dest),
                record["markets"],
            )
        )

        if not market:
            return
        self.redis.set(cache_key, market[0], expiration_in_seconds=Duration.days(1))
        return market[0]

    def get_customer_markets(
        self,
        customer: str,
        origin_in: Optional[Iterable[str]] = None,
        destination_in: Optional[Iterable[str]] = None,
    ) -> Iterable[Market]:
        """Get list of markets for a given customer"""

        match = {}

        if origin_in:
            match["markets.market_origin_city_code"] = {"$in": origin_in}

        if destination_in:
            match["markets.market_destination_city_code"] = {"$in": destination_in}

        pipeline = [
            {"$match": {"customer": customer}},
            {"$unwind": {"path": "$markets"}},
            {"$match": match},
            {"$project": {"_id": 0, "orig": "$markets.market_origin_city_code", "dest": "$markets.market_destination_city_code"}},
        ]

        return self.aggregate(pipeline)

    def check_customer_market_exists(self, customer: str, orig: str, dest: str) -> bool:
        doc = self._db[self.collection].find_one(
            {
                "customer": customer,
                "markets": {"$elemMatch": {"market_origin_city_code": orig, "market_destination_city_code": dest}},
            }
        )

        result = bool(doc)
        return result

    def add_market_to_customer(self, customer: str, market: Dict):
        coll = self._db[self.collection]
        doc = coll.find_one({"customer": customer})
        update = {"$push": {"markets": market}}
        coll.update_one(doc, update)

    def delete_market_from_customer(self, customer: str, market: Dict):
        coll = self._db[self.collection]
        doc = coll.find_one({"customer": customer})
        update = {"$pull": {"markets": market}}
        coll.update_one(doc, update)
