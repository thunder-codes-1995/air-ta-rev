from base.handler import FieldsHandler


class FAHandler(FieldsHandler):
    def orig_fare_amount_handler(self, row, col):
        return row[col].lstrip("0")
