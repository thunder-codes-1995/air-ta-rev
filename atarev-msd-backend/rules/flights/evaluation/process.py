from dataclasses import dataclass, field
from typing import List, TypedDict

from rules.core.eval import Evaluate
from rules.flights.evaluation.data import FareData, RuleData, ScheduleData
from rules.flights.evaluation.fact import Fact
from rules.flights.evaluation.form import RuleEvaluationForm
from rules.flights.evaluation.query import RemoveOldRecommendationsQuery
from rules.flights.evaluation.utils import SuccessT, competitor_criterica, fail, success
from rules.repository import RuleFailResultRepository, RuleResultRepository

rule_result_repo = RuleResultRepository()
rule_fail_repo = RuleFailResultRepository()

required = [
    "competitor.competitorCode",
    "market.originCityCode",
    "market.destCityCode",
    "cabin.cabinCode",
    "leg.deptDate",
    "competitor.competitorCode",
    "fares.maf",
]


class Resp(TypedDict):
    success_count: int
    fail_count: int
    fares_count: int
    flights_count: int


@dataclass
class Proccess:
    form: RuleEvaluationForm
    success: List[SuccessT] = field(default_factory=list, init=False, repr=False)
    # fail: List[FailT] = field(default_factory=list, init=False, repr=False)
    identifiers: List[str] = field(default_factory=list, init=False, repr=False)

    def evaluate(self) -> Resp:
        fares = FareData(self.form).get()

        if fares.empty:
            return {
                "success_count": 0,
                # "fail_count": 0,
                "fares_count": 0,
                "flights_count": 0,
                "rules_count": 0,
            }

        host_flights = ScheduleData(self.form, int(fares.departure_date.min()), int(fares.departure_date.max())).get()
        rules = list(RuleData(self.form).get())

        for flt in host_flights:
            for rule in rules:
                criteria = competitor_criterica(rule)
                facts = Fact(flight=flt, fares=fares, cabin=self.form.cabin.upper(), competitor_criteria=criteria).get()
                res = Evaluate(rule["conditions"]["all"], facts, required=required)()

                if res.result is True:
                    succ_obj = success(self.form.host_code, self.form.cabin, facts, flt, rule)
                    self.success.append(succ_obj)
                    self.identifiers.append(succ_obj["identifier"])
                    continue

                # self.fail.append(
                #     fail(
                #         rule,
                #         flight=flt,
                #         reason=res.reason,
                #         criteria={
                #             "host_code": self.form.host_code,
                #             "cabin": self.form.cabin,
                #             "departure_date": Date(self.form.departure_date).noramlize(),
                #         },
                #     )
                # )

        rule_result_repo.delete(RemoveOldRecommendationsQuery(self.identifiers).query)

        # if self.fail:
        #     rule_fail_repo.insert(self.fail)

        if self.success:
            rule_result_repo.insert(self.success)

        return {
            "success_count": len(self.success),
            # "fail_count": len(self.fail),
            "fares_count": fares.shape[0],
            "flights_count": len(host_flights),
            "rules_count": len(rules),
        }
