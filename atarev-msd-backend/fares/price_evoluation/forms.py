from wtforms import IntegerField, StringField
from wtforms.validators import Optional

from base.helpers.datetime import Date
from fares.common.form import FareForm
from utils.rules import date_range_rule


class GetPriceEvolution(FareForm):
    dtd = IntegerField("dtd", [Optional()])
    outbound_date = StringField("outbound_date", date_range_rule("outbound_date"))

    def get_norm_date(self) -> int:
        return Date(self.outbound_date.data).noramlize()

    def get_dtd(self) -> int:
        return self.dtd.data or 0
