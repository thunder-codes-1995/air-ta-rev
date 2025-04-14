import redis
import os
import json
from datetime import timedelta

from core.logger import Logger

logger = Logger(__name__)


class RedisCacheClient:
    """Wrapper for redis cache server.
    It has basic methods to:
     - set key-value in cache (with optional expiry time in seconds)
     - get value from cache """

    redis_client = None

    def __init__(self):
        if self.is_redis_enabled():
            self.redis_client = redis.Redis(
                host=os.getenv("REDIS_HOST"),
                port=int(os.getenv("REDIS_PORT")),
                password=os.getenv("REDIS_PASSWORD"),
                charset="utf-8",
                decode_responses=True,
                db=0)
            self._key_prefix = os.getenv("REDIS_KEY_PREFIX")
            self._default_cache_expiration_seconds = int(os.getenv("REDIS_EXPIRATION"))

    def is_redis_enabled(self):
        return os.getenv('REDIS_ENABLED') and os.getenv('REDIS_ENABLED').lower() == 'true'

    def _create_prefixed_key(self, key):
        """To avoid conflicts with other products/clients using same redis instance, we need to prefix our entries """
        return f"{self._key_prefix}_{key}"

    def set(self, key: str, value, expiration_in_seconds=None):
        """Add key-value into cache, with optional expiration time in seconds
        If expiration is not set, default expiration will be used (configurable via .env file)"""
        if not self.is_redis_enabled():
            return None

        cache_expiry_in_seconds = expiration_in_seconds or self._default_cache_expiration_seconds
        expiry_time = timedelta(seconds=int(cache_expiry_in_seconds))

        _key = self._create_prefixed_key(key)
        val = json.dumps(value)
        self.redis_client.set(_key, val, expiry_time)

    def get(self, key: str):
        """get value from cache or None if value was not there"""
        if not self.is_redis_enabled():
            return None
        _key = self._create_prefixed_key(key)

        if not self.redis_client.get(_key):
            return None
        try:
            return json.loads(self.redis_client.get(_key))
        except (json.decoder.JSONDecodeError, TypeError):
            logger.warning(f"got json.decoder.JSONDecodeError while retrieving value from cache for key:[{_key}]")
            return self.redis_client.get(_key)

    def clear_cache(self):
        """clear all key-values from cache that were previously stored there"""
        if not self.is_redis_enabled():
            return None
        all_keys = self.redis_client.keys(self._create_prefixed_key('*'))
        logger.info(f'Cache clear was requested, found {len(all_keys)} keys to be deleted')
        if all_keys:
            self.redis_client.delete(*all_keys)
        logger.info(f'Cache clear completed')