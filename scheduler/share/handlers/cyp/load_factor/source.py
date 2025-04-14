import calendar
import re
from dataclasses import dataclass
from datetime import date

from core.meta import read_meta

from .meta import LoadFactorRESMeta


@dataclass
class LoadFactorSource:
    date: int
    path: str

    def __post_init__(self):
        data = read_meta(meta_class=LoadFactorRESMeta, date=self.date, path=self.path, override_header=True, sep=",").fillna("-")
        # remove empty lines
        data = data[
            (data.dow != "-") & (data.dept_time != "-") & (data.carrier != "-") & (data.flt_num != "-") & (data.dept_date != "-")
        ]
        # data.adj = data.adj.map({"Yes": 1, "No": 0})
        # data.adj.fillna("-", inplace=True)
        # normalize departure time
        data.dept_time = data.dept_time.apply(lambda val: int(val.strip().replace(":", "")))
        # normalize departure date
        data["norm_dept_time"] = data.dept_date.apply(self.__handle_date)
        self.data = data

    def __handle_date(self, val: str) -> int:
        pattern = r"(\d+)([A-Za-z]{3})(\d+)"
        match = re.match(pattern, val)
        day, month, year = int(match.group(1)), list(calendar.month_abbr).index(match.group(2)), 2000 + int(match.group(3))
        return int(date(year=year, month=month, day=day).strftime("%Y%m%d"))
