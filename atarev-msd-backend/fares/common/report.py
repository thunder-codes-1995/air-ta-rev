import csv
from dataclasses import dataclass
from io import StringIO
from typing import List, Optional

import pandas as pd
from flask import make_response


@dataclass
class Report:
    data: pd.DataFrame
    header: List[str]
    file_name: str
    columns: Optional[List[str]] = None

    def get(self):
        si = StringIO()
        cw = csv.writer(si)

        if self.data.empty:
            return None

        df = self.data.copy()
        if self.columns:
            df = df[self.columns]

        cw.writerows([self.header] + [list(dict(item).values()) for _, item in df.iterrows()])
        output = make_response(si.getvalue())
        output.headers["Content-Disposition"] = f"attachment; filename={self.file_name}"
        output.headers["Content-type"] = "text/csv"
        return output
