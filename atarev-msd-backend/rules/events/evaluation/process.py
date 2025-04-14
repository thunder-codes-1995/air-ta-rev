from dataclasses import dataclass, field
from typing import List, TypedDict

from rules.core.eval import Evaluate
from rules.events.evaluation.data import EventData, InventoryData, RuleData
from rules.events.evaluation.fact import Fact
from rules.events.evaluation.form import AlertEvaluationForm
from rules.events.evaluation.query import RemoveOldAlertsQuery
from rules.events.evaluation.utils import SuccessT, success
from rules.repository import RuleResultRepository

rule_result_repo = RuleResultRepository()


class Resp(TypedDict):
    success_count: int
    event_count: int
    date_group_count: int


@dataclass
class Proccess:
    form: AlertEvaluationForm
    success: List[SuccessT] = field(default_factory=list, init=False, repr=False)

    def evaluate(self) -> Resp:
        # i'm converting the rules to list because i will use the value many times (one for each combination),
        # cursors are exhausted after one round
        rules = list(RuleData(self.form.host_code).get())
        events = EventData(self.form.host_code).get()
        s_dates: List[int] = events.start_date.unique().tolist()

        # handle each start_date group separately (e.g: all events starting on 2024-04-01 will be handled together)
        for start_date in s_dates:
            inv = InventoryData(self.form.host_code, start_date).get()

            # handle each country_code,cabin combination separately (e.g: FR-ECO)
            for (c_code, cabin), g_df in inv.groupby(["country_code", "cabin"]):
                targeted_event = events[(events.start_date == start_date) & (events.country_code == c_code)]

                # handle each event separately
                for _, event_g_df in targeted_event.groupby("event_name"):
                    event_data = event_g_df.iloc[0]
                    fact = Fact(event_data, g_df, cabin).get()

                    # compare the rule against all possible combinations
                    for rule in rules:
                        res = Evaluate(rule["conditions"]["all"], fact)()
                        if res.result:
                            self.success.append(success(self.form.host_code, fact, rule))

        if self.success:
            rule_result_repo.delete(RemoveOldAlertsQuery(self.form).query)
            rule_result_repo.insert(self.success)

        return {"success_count": len(self.success), "event_count": events.shape[0], "date_group_count": len(s_dates)}
