import os

os.chdir(os.path.dirname(__file__) or ".")

import argparse
import sys
from datetime import datetime, timedelta

from __handlers.hitit import UpdateInventory
from core.env import HITIT_UPDATE_INVENTORY_DAY_COUNT


class HandleUpdateInventory:
    def from_cli(self, params):
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "start_date",
            help="update data starting from this value, value should have the following format : YYYY-MM-DD",
            type=str,
        )
        parser.add_argument("start_offset", help="how many days to skip", type=int)
        return parser.parse_args(params)

    def update(self):
        args = self.from_cli(sys.argv[1:])
        dt = datetime.strptime(args.start_date, "%Y-%m-%d")
        start_date = dt + timedelta(days=args.start_offset)
        print(start_date.strftime("%Y-%m-%d"))
        handler = UpdateInventory(
            "data2.xml",
            {
                "airlineCode": "PY",
                "dayCount": int(HITIT_UPDATE_INVENTORY_DAY_COUNT) if HITIT_UPDATE_INVENTORY_DAY_COUNT else 30,
                "startDate": start_date.strftime("%Y-%m-%d"),
            },
        )
        handler.update()


if __name__ == "__main__":
    HandleUpdateInventory().update()
