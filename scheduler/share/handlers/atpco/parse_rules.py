from core.db import DB
from core.env import ATPCO_FILES_PATH

db = DB()


class RuleParser:
    seq = [
        3,
        3,
        5,
        2,
        5,
        2,
        8,
        6,
        6,
        4,
        4,
        1,
        4,
        11,
        3,
        1,
        11,
        3,
        1,
        25,
        2,
        1,
        2,
        6,
        5,
        1,
        8,
        4,
        3,
        5,
        16,
        8,
        4,
        3,
        5,
        16,
        18,
        12,
        1,
        5,
        5,
        8,
        3,
        5,
        3,
        18,
        9,
    ]

    lables = [
        "tariff_num",
        "carrier",
        "orig_airport",
        "orig_country",
        "dest_airport",
        "dest_country",
        "class",
        "effective_date",
        "discontinue_date",
        "rule_number",
        "routing_number",
        "type",
        "source",
        "orig_fare_amount",
        "orig_currency_code",
        "origin_num_of_decimals",
        "dest_fare_amount",
        "dest_currency_code",
        "dest_num_of_decimals",
        "something",
        "footnote",
        "directonal_indicator",
        "global_indicator",
        "tariff_effective_date",
        "mpm",
        "something2",
        "origin_addon_class",
        "origin_addon_routing",
        "origin_addon_footnote",
        "origin_addon_gateway",
        "origin_addon_amount",
        "dest_addon_class",
        "dest_addon_routing",
        "dest_addon_footnote",
        "dest_addon_gateway",
        "dest_addon_amount",
        "pub_fare_info",
        "sale_date",
        "action",
        "mcn",
        "old_mcn",
        "something3",
        "link_num",
        "seq_num",
        "something4",
        "change_tag",
        "gfs_filling_advice",
    ]

    def __init__(self, path=ATPCO_FILES_PATH):
        self.path = path
        self.data = []

    def parse(self):
        with open(self.path, "r") as file:
            next(file)  # skip first line (titles)
            for line in file:
                obj = self.handle_line(line)
                self.insert(obj)
        if self.data:
            db.atpco.insert_many(self.data)

    def handle_line(self, line: str):
        obj, start = {}, 0
        for lable, length in zip(self.lables, self.seq):
            obj[lable] = line[start : length + start]
            start += length

        return obj

    def insert(self, obj):
        self.data.append(obj)
        if len(self.data) == 1000:
            print("updating")
            db.atpco.insert_many(self.data)
            self.data = []
