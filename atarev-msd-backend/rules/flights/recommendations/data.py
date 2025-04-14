import calendar
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, TypedDict

from base.helpers.datetime import Time
from configurations.repository import ConfigurationRepository
from rules.flights.options.funcs import get_operators
from rules.repository import RuleRepository
from rules.types import Event, Rule, RuleEntity

config_repo = ConfigurationRepository()
rules_repo = RuleRepository()


class Analysis(TypedDict):
    value: List[int]
    apply: str
    operator: str


class Evnt(TypedDict):
    type: str
    class_rank: str
    value: int


class HoverCriteria(TypedDict):
    effective_time: List[str]
    competitor_range: List[str]
    competitor: str
    dtd: List[int]
    dow: Optional[List[str]]


class Hover(HoverCriteria):
    ruleName: str
    analysis: List[Analysis]
    event: Evnt


@dataclass
class HoverDataMap:

    host_code: str

    def get(self) -> Dict[str, Hover]:
        res = {}
        criteria_map = self.get_criteria_map()
        class_rank_map = self.get_classrank_map()
        operators_map = self.get_operators_map()

        mp = {
            "effective_time": self.effective_time,
            "competitor_range": self.competitor_range,
            "competitor": self.competitor,
            "dtd": self.dtd,
            "dow": self.dow,
            "analysis": lambda val: self.analysis(val, criteria_map, operators_map),
        }

        c = rules_repo.find({"carrier": self.host_code, "isActive": True})

        for item in c:
            obj = self.__handle_item(item, mp)
            obj["ruleName"] = item["ruleName"]
            obj["event"] = self.event(item["event"], class_rank_map)
            res[str(item["_id"])] = obj

        return res

    def __handle_item(self, obj: RuleEntity, field_method_map: Dict[str, Callable]) -> HoverCriteria:
        res = {}

        for cond in obj["conditions"]["all"]:
            if "field" not in cond:
                if field_method_map.get(cond["fact"]):
                    res[cond["fact"]] = field_method_map.get(cond["fact"])(cond)

            elif field_method_map.get(cond["field"]):
                res[cond["field"]] = field_method_map.get(cond["field"])(cond["all"])

        return res

    def effective_time(self, value: List[Rule]) -> List[str]:
        return [Time(item["value"]).humanize() for item in value]

    def analysis(self, value: List[Rule], criteria_map: Dict[str, str], operators_map: Dict[str, str]) -> List[Analysis]:
        return [
            {"apply": criteria_map[val["path"].split(".")[1]], "value": val["value"], "operator": operators_map[val["operator"]]}
            for val in value
        ]

    def event(self, value: Event, class_rank_map: Dict[str, str]) -> Evnt:
        return {
            "class_rank": class_rank_map.get(value["params"]["class_rank"], value["params"]["class_rank"]),
            "type": value["type"],
            "value": value["params"]["set_avail"],
        }

    def competitor_range(self, value: List[Rule]) -> List[str]:
        return [item["value"] for item in value]

    def dtd(self, value: List[Rule]) -> List[str]:
        return [item["value"] for item in value]

    def dow(self, value: List[Rule]) -> List[str]:
        return [calendar.day_abbr[item["value"]] for item in value]

    def competitor(self, value: Rule) -> List[str]:
        return value["value"]

    def get_criteria_map(self) -> Dict[str, str]:
        items = config_repo.get_by_key("CRITERIAS", self.host_code)
        return {f"{item['path']}": item["value"] for item in items}

    def get_classrank_map(self) -> Dict[str, str]:
        items = config_repo.get_by_key("CLASS_RANK", self.host_code)
        return {f"{item['key']}": item["value"] for item in items}

    def get_operators_map(self) -> Dict[str, str]:
        items = get_operators()
        return {f"{item['value']}": item["readable"] for item in items}
