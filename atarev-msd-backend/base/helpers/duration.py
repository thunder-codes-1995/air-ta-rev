from enum import Enum


class Interval(Enum):
    MINUTE = 60
    HOUR = 3600
    DAY = 86400


class Duration:
    @classmethod
    def to(cls, dur: Interval, value: int) -> int:
        return dur.value * value

    @classmethod
    def hours(cls, val: int):
        return cls.to(Interval.HOUR, val)

    @classmethod
    def minutes(cls, val: int):
        return cls.to(Interval.MINUTE, val)

    @classmethod
    def days(cls, val: int):
        return cls.to(Interval.DAY, val)

    @classmethod
    def months(cls, val: int):
        return cls.days(30) * val

    @classmethod
    def format(cls, value) -> str:
        """
        takes a normalized duration value (1650) and converted it to
        human readable foramt (16:50)
        """
        val = f"{value}"
        if not val.isnumeric():
            return val

        val = val.zfill(4)
        return f"{val[0:2]}:{val[2:]}"
