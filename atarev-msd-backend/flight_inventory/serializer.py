from dataclasses import dataclass, field

from base.helpers.datetime import Time


@dataclass
class LoadFactorSerializer:
    origin: str
    destination: str
    departure_date: str
    departure_time: int
    load_factor: int
    dept_time: str = field(init=False)
    lf: str = field(init=False)
    cabin: str
    flight_number: str

    def __post_init__(self):
        self.dept_time = Time(self.departure_time).humanize()
        self.lf = f"{self.load_factor}%" if self.load_factor else None
