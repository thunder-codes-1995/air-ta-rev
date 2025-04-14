from dataclasses import dataclass

from base.mongo_utils import convert_list_param_to_criteria, merge_criterions
from fares.repository import FareRepository

from .form import GetFareStructure

repo = FareRepository()


@dataclass
class FareBreakdownQuery:
    form: GetFareStructure

    @property
    def query(self):
        dt_rng = self.form.get_date_range()
        return merge_criterions(
            [
                convert_list_param_to_criteria("marketOrigin", self.form.get_origin()),
                convert_list_param_to_criteria("marketDestination", self.form.get_destination()),
                {"$and": [{"outboundDate": {"$gte": dt_rng[0]}}, {"outboundDate": {"$lte": dt_rng[1]}}]},
            ]
        )
