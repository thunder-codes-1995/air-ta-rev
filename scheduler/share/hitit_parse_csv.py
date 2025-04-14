import os

os.chdir(os.path.dirname(__file__) or ".")

from datetime import datetime

from __handlers.hitit import CsvHandler
from core.db import DB
from core.mongo.connect import MongoConnection


def upload():
    today = datetime.today().strftime("%Y%m%d")
    client = MongoConnection().client
    today_int = int(today)
    db = DB()

    # Start a client session
    with client.start_session() as session:
        # Start a transaction
        with session.start_transaction():
            # try:
            #     db.schedule.delete_many({})
            #     segments = CsvHandler(today_int).parse()
            #     db.schedule.insert_many([obj.json for obj in segments])
            #     print("Done")
            # except Exception as e:
            #     print(e)
            #     session.abort_transaction()
            # else:
            #     # Commit the transaction if there were no errors
            #     session.commit_transaction()
            db.schedule.delete_many({})
            segments = CsvHandler(today_int).parse()
            db.schedule.insert_many([obj.json for obj in segments])
            print("Done")


upload()
