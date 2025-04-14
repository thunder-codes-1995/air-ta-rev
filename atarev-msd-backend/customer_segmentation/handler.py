from base.constants import Constants
from base.handler import FieldsHandler


class CustomerSegmentationGraphsHandler(FieldsHandler):
    def cat_name_handler(self, row, col):
        if row["cat_type"] == "dow_bd":
            return Constants.IDX2DAY[row[col]]
        if row["cat_type"] == "class_bd":
            return {"Y": "ECO", "C": "BUS"}.get(row[col], "UNK")
        return row[col]
