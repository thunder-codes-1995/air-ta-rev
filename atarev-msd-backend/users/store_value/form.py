from typing import List

from pydantic import BaseModel, validator

from events.table.fields import API


class KPIForm(BaseModel):
    fields: List[str]

    @validator("fields")
    def validate_fields(cls, value):
        accepted_fields = ["passenger", "revenue", "average_fare", "capacity", "rask", "cargo"]

        if len(value) != 6:
            raise ValueError(f"all fields are required : {'-'.join(accepted_fields)}")

        if any(field not in accepted_fields for field in value):
            raise ValueError(f"Invalid fields, accepted fields are: {'-'.join(accepted_fields)}")

        return value


class EventTableFields(BaseModel):
    fields: List[str]

    @validator("fields")
    def validate_fields(cls, value):
        if any(val not in API.all() for val in value):
            raise ValueError(f"Invalid fields, accepted fields are: {'-'.join(API.all())}")
        return value
