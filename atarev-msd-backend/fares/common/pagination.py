import math
from dataclasses import dataclass
from typing import Optional, TypedDict, Union

import pandas as pd


class Page(TypedDict):
    data: pd.DataFrame
    prev: Union[None, int]
    next: Union[None, int]
    current: int
    page_count: int


class PaginationMeta(TypedDict):
    currentPage: Optional[int]
    itemsPerPage: Optional[int]
    totalItems: Optional[int]
    totalPages: Optional[int]


@dataclass
class Paginator:
    page: int
    chunk_size: int
    data: pd.DataFrame

    def get(self) -> Page:
        offset = (self.page * self.chunk_size) - self.chunk_size
        target = self.data.iloc[offset : offset + self.chunk_size]

        return {
            "data": target,
            "current": self.page,
            "prev": None if self.page == 1 else self.page - 1,
            "next": None if self.page * self.chunk_size >= self.data.shape[0] else self.page + 1,
            "page_count": math.ceil(self.data.shape[0] / self.chunk_size),
        }


def generate_meta(
    current_page: Optional[int] = 1,
    per_page: Optional[int] = None,
    total: Optional[int] = None,
    total_pages: Optional[int] = None,
) -> PaginationMeta:
    return {
        "currentPage": current_page,
        "itemsPerPage": per_page,
        "totalItems": total,
        "totalPages": total_pages,
    }
