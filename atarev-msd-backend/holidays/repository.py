from typing import List

import pandas as pd

from base.helpers.duration import Duration
from base.repository import BaseRepository


class HolidayRepository(BaseRepository):
    collection = "holidays"

    def get_for_countries(self, codes: List[str]):
        """
        takes-in a list of normalized country code
        and returns holiday data for these countries
        """
        codes = codes or []
        codes.sort()
        cache_key = "holidays_" + "_".join(codes)

        if self.redis.get(cache_key):
            self.redis.get(cache_key)

        data = self.stringify(list(self.find({"country_name": {"$in": codes}})))  # [None]
        self.stringify(self.redis.set(cache_key, data, expiration_in_seconds=Duration.months(1)))
        return data

    def get_by_filter(self, filter_param, as_df=False):
        result = self.stringify(list(self.aggregate(filter_param)))
        if as_df:
            return pd.DataFrame(result)
        return result
