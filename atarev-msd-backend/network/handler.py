from flask import request

from base.entities.currency import Currency
from base.handler import FieldsHandler
from utils.funcs import get_market_carrier_map, split_string


class NetworkHandler(FieldsHandler):
    CARRIER_COLOR_MAP = None

    def __init__(self):
        origin_codes = split_string(request.args.get("orig_city_airport", ""))
        dest_codes = split_string(request.args.get("dest_city_airport", ""))
        host = request.user.carrier
        self.CARRIER_COLOR_MAP = get_market_carrier_map(origin_codes, dest_codes, host)

    def dom_op_al_code_handler(self, row, col):
        return {"color": self.CARRIER_COLOR_MAP[row[col]] if row[col] in self.CARRIER_COLOR_MAP else "#ffffff", "value": row[col]}

    def blended_rev_handler(self, row, col):
        if row.get("currency"):
            return {
                "displayVal": Currency.attach_currency(f"{int(row[col]):,}", row["currency"]),
                "value": int(row[col]),
            }
        return {
            "displayVal": Currency.attach_currency(f"{int(row[col]):,}", "USD"),
            "value": int(row[col]),
        }

    def pax_sum_handler(self, row, col):
        return {"displayVal": f"{row[col]}", "value": row[col]}

    def pax_handler(self, row, col):
        return {"displayVal": f"{row[col]}", "value": row[col]}

    def path_handler(self, row, col):
        if row["bound"] == "Inbound":
            val = row[col]
            val = val.split("-")
            return f"{val[1]}-{val[0]}"
        return row[col]


class NetworkBeyondPointsHandler(FieldsHandler):
    def pax_handler(self, row, col):
        return self.seprate_thousands(row[col])

    def blended_rev_handler(self, row, col):
        return "$" + self.seprate_thousands(row[col])

    def blended_fare_handler(self, row, col):
        return "$" + self.seprate_thousands(row[col])

    def is_direct_handler(self, row, col):
        return row[col]
