from enum import Enum

from core.env import DB_NAME
from core.mongo.connect import MongoConnection
from pymongo.database import Collection, Database


class Collection(Enum):
    CONFIGURATION = "configuration"
    INVENTORY = "hitit"
    ATPCO = "atpco"
    AUTH_RESULT = "authorization_results"
    FS = "fs"
    DDS = "dds_pgs"
    AIRPORT = "airports"
    CURRENCY = "currencies"


class DB:
    def __init__(self, db_name: str = DB_NAME):
        self.db_name = db_name
        self._db = None

    def _initialize_db_connection(self) -> None:
        print("DB was not connected - lazy connection")
        self._db = MongoConnection()[self.db_name]

    @property
    def db(self) -> Database:
        print("DB getter")
        if self._db is None:
            self._initialize_db_connection()
        return self._db

    @property
    def configuration(self) -> Collection:
        print("get configuration")
        return self.db[Collection.CONFIGURATION.value]

    @property
    def schedule(self) -> Collection:
        return self.db[Collection.INVENTORY.value]

    @property
    def authorization(self) -> Collection:
        return self.db[Collection.AUTH_RESULT.value]

    @property
    def atpco(self) -> Collection:
        return self.db[Collection.ATPCO.value]

    @property
    def fs(self) -> Collection:
        return self.db[Collection.FS.value]

    @property
    def dds(self) -> Collection:
        return self.db[Collection.DDS.value]

    @property
    def airports(self) -> Collection:
        return self.db[Collection.AIRPORT.value]

    @property
    def currency(self) -> Collection:
        return self.db[Collection.CURRENCY.value]
