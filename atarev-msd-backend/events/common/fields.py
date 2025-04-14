from dataclasses import dataclass, field
from typing import List, TypedDict, Union


@dataclass
class Field:
    value: str
    label: Union[str, None] = field(default=None)
    rename: Union[str, None] = field(default=None)
    enabled: bool = field(default=True)


class F(TypedDict):
    label: Union[str, None]
    value: str
    enabled: bool


class FieldGroup(TypedDict):
    label: Union[str, None]
    value: str
    fields: List[F]
    enabled: bool


@dataclass
class Group:
    value: str
    fields: List[Field]
    label: Union[str, None] = field(default=None)
    enabled: bool = field(default=True)

    def values(self) -> List[str]:
        return [field.value for field in self.fields if self.enabled]

    def json(self) -> FieldGroup:
        return {
            "value": self.value,
            "label": self.label,
            "enabled": self.enabled,
            "fields": [{"label": f.label, "value": f.value, "enabled": f.enabled} for f in self.fields],
        }


event_name = Field(label="EVENT", value="event_name")
event_type = Field(label="TYPE", value="type")
event_sub_type = Field(label="SUB-TYPE", value="sub_type")
loc = Field(label="LOCATION", value="city")
cabin = Field(label="CABIN", value=f"cabin")
capacity = Field(label="CPACITY", value=f"cap")
load_factor = Field(label="LOAD FACTOR", value=f"lf")
amf = Field(label="AVG MIN FARE", value=f"avg_min_fare")
