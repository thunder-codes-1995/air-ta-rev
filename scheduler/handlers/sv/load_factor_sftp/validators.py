from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional
import re

@dataclass
class Cabin:
    code: str
    total_booking: int
    total_group_booking: int = 0
    capacity: int = 0
    allotment: Optional[int] = None
    available_seats: Optional[int] = None
    load_factor: int = 0

    def __post_init__(self):
        self.validate()

    def validate(self):
        errors = []
        if not self.code:
            errors.append("Cabin code is missing.")
        if not isinstance(self.total_booking, int):
            errors.append("Total booking must be an integer.")
        if not isinstance(self.capacity, int):
            errors.append("Capacity must be an integer.")
        if not isinstance(self.load_factor, int):
            errors.append("Load factor must be an integer.")
        if errors:
            raise ValueError(f"Cabin validation errors: {errors}")

@dataclass
class Leg:
    origin: str
    destination: str
    flight_number: str
    departure_date: str
    departure_time: str
    cabins: List[Cabin] = field(default_factory=list)
    flight_number_ext: str = "-"
    load_factor: int = 70
    arrival_date: Optional[str] = None
    arrival_time: Optional[str] = None

    def __post_init__(self):
        self.validate()

    def validate(self):
        errors = []
        if not re.match(r'^[A-Z]{3}$', self.origin):
            errors.append("Origin code must be 3 uppercase letters.")
        if not re.match(r'^[A-Z]{3}$', self.destination):
            errors.append("Destination code must be 3 uppercase letters.")
        if not isinstance(self.flight_number, int):
            errors.append("Flight number must be an integer.")
        if not isinstance(self.departure_date, int):
            errors.append("Departure date must be an integer.")
        if not isinstance(self.departure_time, int):
            errors.append("Departure time must be an integer.")
        if errors:
            raise ValueError(f"Leg validation errors: {errors}")

@dataclass
class Flight:
    airline_code: str
    date: int
    departure_date: int
    destination: str
    flight_number: int
    origin: str
    departure_time: int
    flight_departure_date: int
    flight_number_ext: str = "-"
    inserted_at: dict = field(default_factory=lambda: datetime.now())
    legs: List[Leg] = field(default_factory=list)

    def __post_init__(self):
        self.validate()

    def validate(self):
        errors = []

        if not isinstance(self.date, int):
            errors.append("Date must be an integer.")
        if not isinstance(self.departure_date, int):
            errors.append("Departure date must be an integer.")
        if not self.destination:
            errors.append("Destination is missing.")
        if not isinstance(self.flight_number, int):
            errors.append("Flight number must be an integer.")
        if not self.origin:
            errors.append("Origin is missing.")
        if not isinstance(self.departure_time, int):
            errors.append("Departure time must be an integer.")

        if errors:
            raise ValueError(f"Flight validation errors: {errors}")

