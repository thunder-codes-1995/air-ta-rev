from dataclasses import dataclass, field
from datetime import datetime

from core.db import DB as Connection
from core.db import Collection
from pymongo import UpdateOne


@dataclass
class Stream:
    collection: Collection
    chunk_size: int = field(default=1000)
    chunck: list = field(init=False, default_factory=list)
    collection_name: str = field(init=False)

    def __post_init__(self):
        self.collection_name = self.collection.value
        self.connection = Connection()

    def insert(self, obj=None) -> None:
        if obj:
            self.chunck.append({**obj, "inserted_at": datetime.now().replace(microsecond=0)})
        if not obj or len(self.chunck) == self.chunk_size:
            print(f"uploading {len(self.chunck)} records to {self.collection.value}")
            self.connection.db[self.collection_name].insert_many(self.chunck)
            self.chunck = []

    def update(self, obj=None, compare=None, upsert=False) -> None:
        if obj and compare:
            self.chunck.append(
                UpdateOne(compare, {"$set": {**obj, "inserted_at": datetime.now().replace(microsecond=0)}}, upsert=upsert)
            )
        if not obj or len(self.chunck) == self.chunk_size:
            print(f"updating {len(self.chunck)} records to {self.collection.value}")
            self.connection.db[self.collection_name].bulk_write(self.chunck)
            self.chunck = []
