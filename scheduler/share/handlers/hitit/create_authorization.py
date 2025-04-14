from datetime import datetime
from pathlib import Path

import pandas as pd
from core.db import DB
from core.env import HITIT_AUTHORIZATION_STORE_TO, HITIT_USERNAME
from core.logger import Logger


class CreateAuthorization:
    db = DB()
    logger = Logger("logs")

    def __init__(self, store_to: str = HITIT_AUTHORIZATION_STORE_TO):
        self.store_to = store_to

    def create(self):
        cursor = self.db.authorization.find(
            {"$or": [{"sent_at": {"$exists": False}}, {"sent_at": None}]}
        )
        today = datetime.today()
        today_int = int(today.strftime("%Y%m%d"))

        data = [
            {
                "update_date": today_int,
                "username": HITIT_USERNAME,
                "airline_code": item["cabin_params"]["airline_code"],
                "flight_number": item["cabin_params"]["flight_number"],
                "departure_port": item["cabin_params"]["origin"],
                "arrival_port": item["cabin_params"]["destination"],
                "departure_date": item["cabin_params"]["departure_date"],
                "rezervation_class": item["authorization_value"]["class_code"],
                "cabin_class": item["cabin_params"]["cabin_code"],
                "authorization_value": item["authorization_value"]["authorization"],
            }
            for item in cursor
        ]

        self.handle_path()
        if not data:
            self.logger.info("no data found (create authorization)")
            return

        # to avoid duplicate files, add current date/time to a filename
        file_suffix = today.strftime("%Y%m%d%H%M%S")


        pd.DataFrame(data).to_csv(
            # f"{self.store_to}/{today_int}.csv", index=False, header=False)
            f"{self.store_to}/authorization_{file_suffix}.csv",
            index=False,
            header=False,
        )

        file = open(f"{self.store_to}/authorization_{file_suffix}.tag", "w")
        file.close()

        self.db.authorization.update_many(
            {"$or": [{"sent_at": {"$exists": False}}, {"sent_at": None}]},
            {"$set": {"sent_at": int(today.strftime("%Y%m%d%H%M%S"))}},
        )

    def handle_path(self):
        directory_path = Path(self.store_to)
        if not directory_path.exists():
            directory_path.mkdir(parents=True, exist_ok=True)
