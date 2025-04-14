from typing import List, Union

import pandas as pd
from wtforms import StringField
from wtforms.validators import Optional

from fares.availability_trends.forms import GetMinFareTrends
from utils.rules import comma_separated_characters_only, date_range_rule


class GetFlightKeys(GetMinFareTrends):

    lookup = StringField("lookup", [Optional()])
    selected = StringField("selected", [Optional(), *comma_separated_characters_only("selected")])
    outbound_date = StringField("outbound_date", [Optional(), *date_range_rule("outbound_date")])

    def get_date(self) -> Union[int, None]:
        if self.outbound_date.data:
            return int(self.outbound_date.data.replace("-", ""))
        return None

    def get_lookup(self) -> Union[str, None]:
        if self.lookup.data:
            return self.lookup.data.strip()
        return None

    def get_selected(self, data: pd.DataFrame) -> List[str]:
        if not self.selected.data or "all" in self.selected.data.lower():
            return []

        keys = data["keys"].unique().tolist()
        selected = list(filter(lambda val: val in keys, self.list_of_string("selected")))
        return selected
