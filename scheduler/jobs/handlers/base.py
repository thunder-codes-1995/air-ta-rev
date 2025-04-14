from typing import List
import pandas as pd


class FieldsHandler:
    """ 
        some fields need to be handled (shaped in certain form) this handle will
        take care of that 
    """

    def handle_rows(self, df: pd.DataFrame):
        columns = df.columns
        result = []
        for _, row in df.iterrows():
            record = self.__handle_row(row, columns)
            result.append(record)
        return result

    def __handle_row(self, row: pd.Series, cols: List[str]):
        obj = {}
        for col in cols:
            # for every column : 
            # if handler exists (<column_name>_handler) -> run that handler 
            # otherwise get value as is 
            if hasattr(self, f"{col}_handler"):
                method = getattr(self, f"{col}_handler")
                obj[col] = method(row, col)
            else:
                obj[col] = row[col]
        return obj
