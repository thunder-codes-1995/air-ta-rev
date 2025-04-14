import os
import certifi
import pymongo
from dotenv import load_dotenv

load_dotenv()

class MongoWrapper:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(MongoWrapper, cls).__new__(cls, *args, **kwargs)
            cls._instance._lazy_init()
        return cls._instance

    def _lazy_init(self):
        if not hasattr(self, 'dbConn') or self.dbConn is None:
            connection_string = f'mongodb+srv://{os.getenv("DB_USER")}:{os.getenv("DB_PASSWORD")}@{os.getenv("DB_HOST")}/?retryWrites=true&w=majority'
            mongo_client = pymongo.MongoClient(connection_string, tlsCAFile=certifi.where())
            self.dbConn = mongo_client[os.getenv("DB_NAME")]

    def get_mongo_client(self):
        return self.dbConn

    def col_scraped_fares_transformed(self):
        return self.get_mongo_client()["scraped_fares_transformed"]

    def col_fares_processed(self):
        return self.get_mongo_client()["fares2"]

    def col_lfa_schedule(self):
        return self.get_mongo_client()["schedule"]

    def col_exchange_rates(self):
        return self.get_mongo_client()["exchange_rates"]

    def col_msd_schedule(self):
        return self.get_mongo_client()["msd_schedule"]

    def col_msd_config(self):
        return self.get_mongo_client()["configuration"]

    def col_dds(self):
        return self.get_mongo_client()["dds_pgs"]

    def col_airports(self):
        return self.get_mongo_client()["airports"]

    def col_events(self):
        return self.get_mongo_client()["events"]

    def col_cities(self):
        return self.get_mongo_client()["cities"]