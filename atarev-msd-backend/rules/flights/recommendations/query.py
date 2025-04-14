from dataclasses import dataclass

from base.mongo_utils import convert_list_param_to_criteria, merge_criterions
from rules.flights.recommendations.form import RecommendationForm


@dataclass
class RecommendationsQuery:
    host_code: str
    form: RecommendationForm

    @property
    def query(self):

        return merge_criterions(
            [
                convert_list_param_to_criteria("carrier", self.host_code),
                convert_list_param_to_criteria("facts.market.originCode", self.form.get_origin()),
                convert_list_param_to_criteria("facts.market.destCode", self.form.get_destination()),
                convert_list_param_to_criteria(
                    "facts.cabin.cabinCode",
                    {
                        "$regex": f"{self.form.get_cabin(False)[0]}",
                        "$options": "i",
                    },
                ),
            ]
        )
