from typing import Any, Dict, Union

from dotenv import load_dotenv

from base.db_connection import DBConnection
from base.redis import Redis

load_dotenv()


class BaseRepository:
    _collection = None
    _columns: Dict[str, str] = {}
    _redis = Redis()
    _db = DBConnection().get_connection()

    def __init__(self, collection: Union[str, None] = None):
        if collection:
            self._collection = collection

    @property
    def collection(self) -> str:
        return self.collection

    # @measure_time(message="BaseRepository::find_one()")
    def find_one(self, filter={}, sort=[]):
        # logger.debug(f"mongo find_one, collection:{self.collection}", {"query": filter})
        result = self._db[self.collection].find_one(filter, sort=sort)
        return result

    def find(self, filter={}):
        # logger.debug(f"mongo find, collection:{self.collection}", {"query": filter})
        result = self._db[self.collection].find(filter)
        return result

    # @measure_time(message="BaseRepository::aggregate()")
    def aggregate(self, pipline):
        # logger.debug(f"mongo aggregate, collection:{self.collection}", {"query": pipline})
        result = self._db[self.collection].aggregate(pipline)
        return result

    def get_column(self, key: str) -> str:
        """get a column by key (will return the same key if value not found)"""
        return self.columns.get("key") or key

    @property
    def redis(self):
        return self._redis

    def stringify(self, data):
        """convert all Object_id in response to string id"""
        if not data:
            return
        if type(data) is dict and data.get("_id"):
            data["id"] = str(data["_id"])
            del data["_id"]
        else:
            for item in data:
                if item.get("_id"):
                    item["id"] = str(item["_id"])
                    del item["_id"]
        return data

    # @measure_time(message="BaseRepository::insert()")
    def insert(self, data):
        # logger.debug(f"mongo insert, collection:{self.collection}")
        result = (self._db[self.collection]).insert_many(data)
        return result

    # @measure_time(message="BaseRepository::update_one()")
    def update_one(self, filter, data):
        # logger.debug(f"mongo update_one, collection:{self.collection}", {"query": filter})
        result = (self._db[self.collection]).update_one(filter, {"$set": data})
        return result

    def update(self, filter, data):
        # logger.debug(f"mongo update_one, collection:{self.collection}", {"query": filter})
        result = (self._db[self.collection]).update_many(filter, {"$set": data})
        return result

    def bulk_write(self, data):
        (self._db[self.collection]).bulk_write(data)

    def delete(self, match: Dict[str, Any]) -> None:
        (self._db[self.collection]).delete_many(match)
