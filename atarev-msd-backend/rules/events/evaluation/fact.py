from dataclasses import dataclass
from typing import Dict

import pandas as pd

from rules.events.evaluation.data import bins
from rules.events.evaluation.types import EventFact, Fct, Pickup


def pickup(data: pd.Series) -> Dict[str, Pickup]:
    """
    calculate pickup value for list of bins
    e.g :
        pickup 270 = pickup 270 - pickup 365
        pickup ratio 270 = pickup 270 - pickup 365 / pickup 365
    """

    dte = data.dte.tolist()
    lf = data.lf.tolist()
    i, j = 0, 1
    res: Dict[str, Pickup] = {}

    while j < len(dte):
        pred_lf, succ_lf = lf[i], lf[j]
        pred_dte, succ_dte = dte[i], dte[j]
        pkup: Pickup = {"value": 0, "ratio": 0}

        if pred_lf == 0:
            pkup["value"] = int(succ_lf)
            pkup["ratio"] = int(100)
        else:
            pkup["value"] = int(pred_lf - succ_lf)
            pkup["ratio"] = int(round((pred_lf - succ_lf) / pred_lf))

        res[f"{pred_dte}_{succ_dte}"] = pkup
        i += 1
        j += 1

    return res


def event_fact(data: pd.Series) -> EventFact:
    return {
        "event_name": data.event_name,
        "country_code": data.country_code,
        "type": data.type,
        "sub_type": data.sub_type,
        "start_date": int(data.start_date),
        "end_date": int(data.end_date),
        "id": str(data.id),
        "city": data.city or None,
    }


def flight_fact(data: pd.Series) -> Dict[str, Pickup]:
    return pickup(data)


@dataclass
class Fact:
    event: pd.Series
    inventory: pd.DataFrame
    cabin: str

    def get(self) -> Fct:
        df = self.__fill()
        return {"event": event_fact(self.event), "flight": {"cabin": self.cabin, "pickup": {"lf": flight_fact(df)}}}

    def __fill(self):
        df = pd.DataFrame({"dte": bins})
        m = df.merge(self.inventory, on="dte", how="outer")
        m.lf = m.lf.fillna(0)
        return m
