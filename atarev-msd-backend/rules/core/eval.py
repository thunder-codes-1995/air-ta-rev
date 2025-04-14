from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, List, Optional, Union, cast

import pandas as pd

from rules.core.operators import And, Or
from rules.core.utils import parse
from rules.types import Rule, SubRule

mp = {
    "in": "_in",
    "equal": "eq",
    "greaterThanInclusive": "gte",
    "lessThanInclusive": "lte",
    "lessThan": "lt",
    "greaterThan": "gt",
}


@dataclass
class FailReason:
    rule: Union[Rule, SubRule]
    obj: Dict[str, Any]

    def generate(self) -> Dict[str, Any]:
        if "all" in self.rule or "any" in self.rule:
            df = pd.DataFrame(self.rule.get("all") or self.rule.get("any"))
            df["input_value"] = df.path.apply(lambda p: parse(self.obj, p))

            operator = df.operator.unique().tolist()
            path = df.path.unique().tolist()
            accept = df.value.unique().tolist()
            value = df.input_value.unique().tolist()

        else:
            operator = self.rule["operator"]
            path = self.rule["path"]
            accept = self.rule["value"]
            value = parse(self.obj, path)

        return {"operator": operator, "path": path, "value": value, "accept": accept}


@dataclass
class Evaluation:
    result: bool
    reason: Union[Dict[str, Any], None]


@dataclass
class Evaluate:
    rules: Union[List[Rule], List[SubRule]]
    obj: Dict[str, Any]
    required: Optional[List[str]] = field(default_factory=list)

    def __call__(self, *args: Any, **kwds: Any) -> Evaluation:
        valid: bool = True
        idx = 0
        rule = None
        res = None
        reason = None

        while valid and idx < len(self.rules):
            rule = self.rules[idx]
            if rule.get("field") and rule.get("all"):
                valid = self._and(cast(List[Rule], rule["all"]), self.obj)

            elif rule.get("field") and rule.get("any"):
                valid = self._or(cast(List[Rule], rule["any"]), self.obj)

            else:
                valid = self.condition(cast(Rule, rule), self.obj)()

            if not valid:
                reason = FailReason(rule=rule, obj=self.obj).generate()
                valid = False

            idx += 1

        res = Evaluation(valid, reason)
        return res

    def condition(self, rule: Rule, obj: Dict[str, Any]) -> Callable:
        value = parse(obj, rule["path"])

        if value is None:
            if self.required and rule["path"] in self.required:
                return lambda: False
            return lambda: True

        method: Callable = getattr(self, mp.get(rule["operator"], rule["operator"]))
        return method(value, rule["value"])

    def _in(self, val: Any, iter: Iterable) -> Callable:
        return lambda: val in iter

    def gt(self, val: Union[int, float], compare: Union[int, float]) -> Callable:
        return lambda: val > compare

    def gte(self, val: Union[int, float], compare: Union[int, float]) -> Callable:
        return lambda: val >= compare

    def lt(self, val: Union[int, float], compare: Union[int, float]) -> Callable:
        return lambda: val < compare

    def lte(self, val: Union[int, float], compare: Union[int, float]) -> Callable:
        return lambda: val <= compare

    def eq(self, val: Union[str, float, int, bool], compare: Union[str, float, int, bool]) -> Callable:
        return lambda: type(val) is type(compare) and val == compare

    def _and(self, rules: List[Rule], obj: Dict[str, Any]) -> bool:
        return And([self.condition(rule, obj) for rule in rules])()

    def _or(self, rules: List[Rule], obj: Dict[str, Any]) -> bool:
        return Or([self.condition(rule, obj) for rule in rules])()
