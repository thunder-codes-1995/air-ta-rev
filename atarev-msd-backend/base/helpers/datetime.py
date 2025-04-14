import calendar
from dataclasses import dataclass
from datetime import date, datetime
from typing import Union


@dataclass
class Time:
    value: Union[int, str]

    def __post_init__(self):
        if type(self.value) is int:
            assert len(f"{self.value}") <= 4, "invalid time value"

        if type(self.value) is str:
            assert len(f"{self.value}") <= 5, "invalid time value"

    def humanize(self) -> str:
        value = f"{self.value}"
        if len(value) == 2:
            value = f"00{self.value}"
        value = value.zfill(4) if len(value) >= 3 else f"{str(int(value) * 100)}".zfill(4)
        return f"{value[0:2]}:{value[2:4]}"

    def normalize(self) -> int:
        assert type(self.value) is str
        return int(self.value.replace(":", ""))


@dataclass
class Date:
    value: Union[int, str, date, datetime]

    def __post_init__(self):
        if type(self.value) is int:
            assert len(f"{self.value}") == 8, "invalid date value"

        if type(self.value) is str:
            assert len(f"{self.value}") == 10, "invalid date value"

    def humanize(self) -> str:
        if type(self.value) is date:
            return self.value.isoformat()

        if type(self.value) is datetime:
            return self.value.strftime("%Y-%m-%d")

        value = f"{self.value}"
        return f"{value[0:4]}-{value[4:6]}-{value[6:]}"

    def noramlize(self) -> int:
        if type(self.value) is date:
            return int(self.value.isoformat().replace("-", ""))

        if type(self.value) is datetime:
            return int(self.value.strftime("%Y%m%d"))

        assert type(self.value) is str
        return int(self.value.replace("-", ""))

    def date(self) -> date:
        if type(self.value) is str:
            return datetime.strptime(self.value, "%Y-%m-%d").date()
        return datetime.strptime(self.humanize(), "%Y-%m-%d").date()

    def weekday_abbr(self) -> str:
        val = self.value if type(self.value) is str else self.humanize()
        weekday_int = datetime.strptime(val, "%Y-%m-%d").weekday()
        return calendar.day_abbr[weekday_int]
