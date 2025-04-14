from typing import Any, Dict, List


def parse(obj: Dict[str, Any], field: str) -> Any:
    path: List[str] = field.split(".")
    key: str = path[0]

    if not obj.get(key):
        return

    if len(path) == 1:
        return obj[key]

    return parse(obj[key], ".".join(path[1:]))
