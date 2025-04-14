import contextlib
import time
import uuid

from pymongo import errors
from datetime import datetime, timedelta


@contextlib.contextmanager
def lock(collection, key, wait=30, poll_period=5, lease_period=30):

    lock = MongoLock(collection, key, lease_period)
    try:
        lock.acquire(wait=wait, poll_period=poll_period)
        yield lock
    finally:
        lock.release()


class MongoLock(object):

    def __init__(self, collection, lock_name, lease=120):
        self.collection = collection
        self.lock_name = lock_name
        self._client_id = str(uuid.uuid4())
        self._locked = False
        self._lease_time = lease
        self._lock_expires = False

    @property
    def locked(self):
        if not self._locked:
            return self._locked
        return self._locked and (datetime.utcnow() < self._lock_expires)

    @property
    def client_id(self):
        return self._client_id

    def acquire(self, wait=None, poll_period=5):
        result = self._acquire()
        if not wait:
            return result

        assert isinstance(wait, int)
        max_wait = datetime.utcnow() + timedelta(wait)
        while max_wait < datetime.utcnow():
            result = self._acquire()
            if result:
                return result
            time.sleep(poll_period)

    def _acquire(self):
        ttl = datetime.utcnow() + timedelta(seconds=self._lease_time)
        try:
            self.collection.insert_one({
                '_id': self.lock_name,
                'ttl': ttl,
                'client_id': self._client_id})
        except errors.DuplicateKeyError:
            self.collection.delete_one(
                {"_id": self.lock_name, 'ttl': {'$lt': datetime.utcnow()}})
            try:
                self.collection.insert_one(
                    {'_id': self.lock_name,
                     'ttl': ttl,
                     'client_id': self._client_id})
            except errors.DuplicateKeyError:
                self._locked = False
                return self._locked
        self._lock_expires = ttl
        self._locked = True
        return self._locked

    def release(self):
        if not self._locked:
            return False
        self.collection.delete_one(
            {"_id": self.lock_name, 'client_id': self._client_id})
        self._locked = False
        return True