from typing import Any, Callable, Dict, List, Union

from rules.types import CondtionMap, Rule, SubRule


def rng(**kwargs) -> SubRule:
    _and = [
        {
            "fact": kwargs.get("fact"),
            "path": kwargs.get("path"),
            "value": kwargs.get("value")[0],
            "operator": "greaterThanInclusive",
        },
        {
            "fact": kwargs.get("fact"),
            "path": kwargs.get("path"),
            "value": kwargs.get("value")[1],
            "operator": "lessThanInclusive",
        },
    ]
    return {"field": kwargs.get("field"), "and": _and}


method_callable_map: Dict[str, Callable] = {"rng": rng}


def conditions(data: Dict[str, Any], condtion_map=List[CondtionMap]) -> List[Union[Rule, SubRule]]:
    conditions = []

    for obj in condtion_map:
        params = dict(path=obj["path"], fact=obj["fact"], value=data[obj["name"]])

        if data.get(obj["name"]) is None:
            continue

        if method_callable_map.get(obj["operator"]):
            callable = method_callable_map[obj["operator"]]
            res = callable(**{**params, "field": obj["field"]})
            conditions.append(res)
            continue

        conditions.append({**params, "operator": obj["operator"]})

    return conditions
