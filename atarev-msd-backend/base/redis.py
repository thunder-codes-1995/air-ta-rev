import json
import os

import redis
from flask import request

# from utils.logger import Logger

# logger = Logger(__name__)


class Redis:
    redis_client = None

    def __init__(self):
        # logger.debug(f'Initialize redis client, is_redis_enabled={self.is_redis_enabled()}, REDIS_HOST={os.getenv("REDIS_HOST")}')
        if self.is_redis_enabled():
            self.redis_client = redis.Redis(
                host=os.getenv("REDIS_HOST"),
                port=os.getenv("REDIS_PORT"),
                password=os.getenv("REDIS_PASSWORD"),
                charset="utf-8",
                decode_responses=True,
                db=0,
            )

    def is_redis_enabled(self):
        return os.getenv("REDIS_ENABLED") and os.getenv("REDIS_ENABLED").lower() == "true"

    def create_prefixed_key(self, key, skip_host=False):
        prefix = os.getenv("REDIS_KEY_PREFIX") or "defaultprefix"
        try:
            host = request.user.carrier
        except RuntimeError:
            host = "no_request_context"

        if skip_host == True:
            return f"{prefix}_{key}"
        return f"{prefix}_{host}_{key}"

    def set(self, key: str, value, expiration_in_seconds=None, generate_prefix=True):
        if not self.is_redis_enabled():
            return None

        val = json.dumps(value)
        _key = self.create_prefixed_key(key) if generate_prefix else ""
        self.redis_client.set(_key, val, expiration_in_seconds)

    def get(self, key: str, generate_prefix=True):
        if not self.is_redis_enabled():
            return None
        _key = self.create_prefixed_key(key) if generate_prefix else key

        if not self.redis_client.get(_key):
            return
        try:
            return json.loads(self.redis_client.get(_key))
        except json.decoder.JSONDecodeError:
            return self.redis_client.get(_key)

    def clear(self):
        if not self.is_redis_enabled():
            return None

        all_keys = self.redis_client.keys(self.create_prefixed_key("*", True))
        # logger.info(f"Cache clear was requested, found {len(all_keys)} keys to be deleted")
        if all_keys:
            self.redis_client.delete(*all_keys)
        all_keys = self.redis_client.keys(self.create_prefixed_key("*", True))
        # logger.info(f"Cache clear completed")

    def delete(self, key: str):
        self.redis_client.delete(key)
