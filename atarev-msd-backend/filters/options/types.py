from typing import List, TypedDict


class Option(TypedDict):
    value: str


class SimpleOnD(TypedDict):
    origin: List[Option]
    destination: List[Option]


class SubCategory(TypedDict):
    value: str


class Category:
    value: str
    sub_categories: List[SubCategory]
