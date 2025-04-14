from typing import List

from wtforms import StringField
from wtforms.validators import DataRequired, Optional

from base.forms import Form
from base.helpers.cabin import CabinMapper
from utils.rules import comma_separated_characters_only, date_range_rule


class GetFareStructure(Form):
    cabin = StringField("cabin", [*comma_separated_characters_only("cabin"), Optional()])
    origin = StringField("origin", [DataRequired()])
    destination = StringField("destination", [DataRequired()])
    date_range_start = StringField("date_range_start", date_range_rule("date_range_end"))
    date_range_end = StringField("date_range_end", date_range_rule("date_range_end"))

    def get_origin(self) -> List[str]:
        return self.origin.data.split(",")

    def get_destination(self) -> List[str]:
        return self.destination.data.split(",")

    def get_cabin(self) -> List[str]:
        if Form.should_skip(self.cabin.data or ""):
            return []
        return [CabinMapper.normalize(c) for c in self.cabin.data.split(",")]
