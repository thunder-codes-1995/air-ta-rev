import time
from dataclasses import dataclass

from core.meta import logger
from jobs.repositories.configuration import ConfigurationRepository
from jobs.lib.utils.redis_cache_client import RedisCacheClient


@dataclass
class RoundTripRateHandler:
    host: str

    def rates(self):
        cache = RedisCacheClient()
        rates = cache.get("RT_RATES")

        if rates:
            return rates

        rates = ConfigurationRepository.get_config_by_key("RT_RATES", self.host) or []
        rates = self._format_rates(rates)
        cache.set("RT_RATES", rates, 60 * 60)
        return rates

    def _format_rates(self, rates):
        return {f"{obj['origin']}-{obj['destination']}": {"rate": obj["rate"], "carriers": obj.get("carriers")} for obj in rates}
