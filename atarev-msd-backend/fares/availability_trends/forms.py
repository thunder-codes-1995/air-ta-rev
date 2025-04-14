from datetime import datetime, timedelta
from typing import Tuple

from wtforms import StringField
from wtforms.validators import Optional

from fares.common.form import FareForm
from utils.rules import date_range_rule


class GetMinFareTrends(FareForm):
    date_range_start = StringField("date_range_start", [Optional(), *date_range_rule("date_range_start")])
    date_range_end = StringField("date_range_end", [Optional(), *date_range_rule("date_range_start")])

    def get_date_range(self) -> Tuple[int, int]:
        today = datetime.today().date()
        future = today + timedelta(days=120)

        if self.date_range_start.data and self.date_range_end.data:
            return (int(self.date_range_start.data.replace("-", "")), int(self.date_range_end.data.replace("-", "")))

        if self.date_range_start.data:
            return int(self.date_range_start.data.replace("-", "")), int(future.strftime("%Y%m%d"))

        if self.date_range_end.data:
            return (int(today.strftime("%Y%m%d")), int(self.date_range_end.data.replace("-", "")))

        return int(today.strftime("%Y%m%d")), int(future.strftime("%Y%m%d"))
