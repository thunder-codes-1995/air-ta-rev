from typing import List

from events.common.fields import F, FieldGroup


class Label(F):
    selected: bool


class LableGroup(FieldGroup):
    fields: List[Label]
