import os
import threading

import certifi
import pymongo
from dotenv import load_dotenv

lock = threading.Lock()


load_dotenv()

host = os.getenv("DB_HOST")
password = os.getenv("DB_PASSWORD")
user = os.getenv("DB_USER")
database = os.getenv("DB_NAME")
connection_string = f"mongodb+srv://{user}:{password}@{host}/{database}?ssl=true&retryWrites=true&w=majority"
client = pymongo.MongoClient(connection_string, tlsCAFile=certifi.where())


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            with lock:
                if cls not in cls._instances:
                    cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class DBConnection(metaclass=Singleton):
    def __init__(self):
        self.__connection = client[database]

    def get_connection(self):
        return self.__connection
