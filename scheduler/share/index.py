# from scripts.hitit_create_authorization import create_authorization
# from scripts.hitit_parse_csv import upload
# from scripts.hitit_update_inventory import update_inventory
import pandas as pd
from __handlers.atpco.parse_rules import RuleParser
from __handlers.cyp.fare_structure.parser import Handler as FareStrucutreHandler
from __handlers.cyp.load_factor.parser import Handler

# from handlers.hitit.daily_csv import CsvHandler

# from handlers.hitit.update_inventory import UpdateInventory


if __name__ == "__main__":
    ...
    # print(pd.read_csv("handlers/cyp/samples/filed_fares.20230719.csv", usecols=))
    FareStrucutreHandler(20230719).parse()
    # RuleParser("handlers/atpco/samples/data.txt").parse()
    # print(pd.read_csv(f"handlers/cyp/samples/loadfactor.20230726.csv", header=None, skiprows=1))
