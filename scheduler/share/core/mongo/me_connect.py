from pymongo import MongoClient
import mongoengine as me
from share.core.env import get_env_key

class MEConnect():
    is_connected = False

    def connect(self):
        if MEConnect.is_connected:
            return
        db_name = get_env_key('DB_NAME')
        db_user = get_env_key('DB_USER')
        db_host = get_env_key('DB_HOST')
        db_pw = get_env_key('DB_PASSWORD')
        host = f'mongodb+srv://{db_user}:{db_pw}@{db_host}/{db_name}'
        me.connect(host=host)
        MEConnect.is_connected = True

    def create_and_connect_to_test_db(self):
        db_name = get_env_key('DB_NAME')
        client = MongoClient('mongodb://localhost', 27017)
        db = client[db_name]
        if not MEConnect.is_connected:
            me.connect(db_name)
            MEConnect.is_connected = True
        return db
        