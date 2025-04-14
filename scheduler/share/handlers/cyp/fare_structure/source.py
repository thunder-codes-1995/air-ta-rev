from dataclasses import dataclass

from core.meta import read_meta

from .meta import FareStructureRESMeta


@dataclass
class FareStrucutreSource:
    date: int
    path: str

    def __post_init__(self):
        data = read_meta(FareStructureRESMeta, self.date, self.path, override_header=True).fillna("-")

        data.fare_basis = data.fare_basis.apply(lambda val: val.strip())
        data.t_dow = data.t_dow.astype(str)

        data["normalized_first_ticketing_date"] = data.first_ticketing_datetime.apply(self.__normalize_dt)
        data["normalized_last_ticketing_date"] = data.last_ticketing_datetime.apply(self.__normalize_dt)
        data["normalized_observed_at"] = data.observed_at.apply(self.__handle_observed).apply(self.__normalize_dt)

        self.data = data

    def __normalize_dt(self, val: str):
        if not val.strip():
            return "-"

        if val == "-":
            return val

        return int(val.split(" ")[0].replace("/", "").replace("-", ""))

    def __handle_observed(self, val: str):
        return val.replace("#", "")
