from typing import Any, Dict, List, TypedDict

from fares.common.pagination import PaginationMeta


class TableResp(TypedDict):
    meta: PaginationMeta
    data: List[Dict[str, Any]]
    labels: Dict[str, str]
