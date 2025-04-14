# from datetime import datetime

from pydantic import BaseModel, validator

# from base.helpers.datetime import Date


class RuleEvaluationForm(BaseModel):
    origin: str
    destination: str
    host_code: str
    # departure_date: str
    start_datetime: int
    end_datetime: int
    cabin: str

    @classmethod
    def validate_airports(cls, value: str) -> bool:
        airports = value.strip().split(",")
        return all(len(airport) == 3 for airport in airports)

    @validator("origin")
    def validate_origin(cls, value: str) -> str:
        if not cls.validate_airports(value):
            raise ValueError(f"invalid origin value {value} : length == 3 is required")
        return value.upper()

    @validator("destination")
    def validate_destination(cls, value: str) -> str:
        if not cls.validate_airports(value):
            raise ValueError(f"invalid destination value {value} : length == 3 is required")
        return value.upper()

    @validator("host_code")
    def validate_host_code(cls, value: str) -> str:
        if len(value) != 2:
            raise ValueError(f"invalid host_code value {value} : length == 2 is required")
        return value.upper()

    # @validator("departure_date")
    # def validate_departure_date(cls, value: str) -> str:
    #     today = datetime.now().date()
    #     dept_date = datetime.strptime(value, "%Y-%m-%d").date()

    #     if dept_date < today:
    #         raise ValueError(f"date could not be in the past")

    #     return value

    # def get_norm_dept_date(self) -> int:
    #     return Date(self.departure_date).noramlize()
