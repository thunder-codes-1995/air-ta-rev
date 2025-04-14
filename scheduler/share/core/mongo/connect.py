import certifi
import pymongo
from core.env import CONNECTION_STRING
from dotenv import load_dotenv
from pymongo.database import Database

load_dotenv()


class MongoConnection:
    client = None

    def __init__(self):
        connection_string = CONNECTION_STRING
        self.client = pymongo.MongoClient(connection_string, tlsCAFile=certifi.where())

    def __new__(cls):
        if not hasattr(cls, "instance"):
            cls.instance = super().__new__(cls)
        return cls.instance

    def __getitem__(self, key: str) -> Database:
        return self.client[key]
