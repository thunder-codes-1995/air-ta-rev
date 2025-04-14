from typing import List

from attr import field, frozen, validators
from core.fields import number, string


@frozen(kw_only=True)
class Class:
    code: str = string()
    seats_available: int = number()
    total_booking: int = number()
    total_group_booking: int = number()
    parent_booking_class: str = string()
    authorization: int = number()
    display_sequence: int = number()

    @property
    def json(self):
        return {
            "code": self.code,
            "seats_available": self.seats_available,
            "total_booking": self.total_booking,
            "total_group_booking": self.total_group_booking,
            "parent_booking_class": self.parent_booking_class,
            "authorization": self.authorization,
            "display_sequence": self.display_sequence,
        }


@frozen(kw_only=True)
class Cabin:
    code: str = str()
    capacity: int = number()
    allotment: int = number()
    total_booking: int = number()
    total_group_booking: int = number()
    available_seats: int = number()
    overbooking_level: int = number()
    classes: List[Class] = field(factory=list, validator=validators.instance_of(list))

    def _classes(self, **kwargs) -> List[Class]:
        """
        access classes for Cabin object
        if kwargs is not empty filter based on that otherwise return all classes
        """
        if not kwargs:
            return self.classes
        return list(
            filter(
                lambda obj: all(getattr(obj, key) == kwargs[key] for key in kwargs),
                self.classes,
            )
        )

    def has_classes(self, **kwargs) -> bool:
        return (
            len(
                list(
                    filter(
                        lambda obj: any(
                            getattr(obj, key) == kwargs[key] for key in kwargs
                        ),
                        self.classes,
                    )
                )
            )
            > 0
        )

    @property
    def json(self):
        return {
            "code": self.code,
            "capacity": self.capacity,
            "allotment": self.allotment,
            "total_booking": self.total_booking,
            "total_group_booking": self.total_group_booking,
            "available_seats": self.available_seats,
            "overbooking_level": self.overbooking_level,
            "classes": [cls.json for cls in self.classes],
        }


@frozen(kw_only=True)
class Leg:
    origin: str = string()
    destination: str = string()
    flight_number: str = string()
    flight_number_ext: str = string()
    departure_date: int = number()
    arrival_date: int = number()
    departure_time: int = number()
    arrival_time: int = number()
    cabins: List[Cabin] = field(factory=list, validator=validators.instance_of(list))

    def _cabins(self, **kwargs) -> List[Cabin]:
        """
        access cabins for Leg object
        if kwargs is not empty filter based on that otherwise return all cabins
        """
        if not kwargs:
            return self.cabins
        return list(
            filter(
                lambda obj: all(getattr(obj, key) == kwargs[key] for key in kwargs),
                self.cabins,
            )
        )

    def has_cabins(self, **kwargs) -> bool:
        return (
            len(
                list(
                    filter(
                        lambda obj: any(
                            getattr(obj, key) == kwargs[key] for key in kwargs
                        ),
                        self.cabins,
                    )
                )
            )
            > 0
        )

    @property
    def json(self):
        return {
            "origin": self.origin,
            "destination": self.destination,
            "flight_number": self.flight_number,
            "flight_number_ext": self.flight_number_ext,
            "departure_date": self.departure_date,
            "arrival_date": self.arrival_date,
            "departure_time": self.departure_time,
            "arrival_time": self.arrival_time,
            "cabins": [cabin.json for cabin in self.cabins],
        }


@frozen(kw_only=True)
class _Segment:
    airline_code: str = string(min_length=2)
    origin: str = string()
    destination: str = string()
    flight_number: str = string()
    flight_number_ext: str = string()
    flight_departure_date: int = number()
    departure_date: int = number()
    legs: List[Leg] = field(factory=list, validator=validators.instance_of(list))

    @property
    def json(self):
        return {
            "airline_code": self.airline_code,
            "origin": self.origin,
            "destination": self.destination,
            "flight_number": self.flight_number,
            "flight_number_ext": self.flight_number_ext,
            "flight_departure_date": int(self.flight_departure_date),
            "departure_date": int(self.departure_date),
            "legs": [leg.json for leg in self.legs],
        }

    def _legs(self, **kwargs) -> List[Leg]:
        """
        access legs for Schedule object
        if kwargs is not empty filter based on that otherwise return all legs
        """
        if not kwargs:
            return self.legs
        return list(
            filter(
                lambda obj: all(getattr(obj, key) == kwargs[key] for key in kwargs),
                self.legs,
            )
        )

    def has_legs(self, **kwargs) -> bool:
        return (
            len(
                list(
                    filter(
                        lambda obj: any(
                            getattr(obj, key) == kwargs[key] for key in kwargs
                        ),
                        self.legs,
                    )
                )
            )
            > 0
        )


class Segment:
    def __init__(self, data):
        self.data = _Segment(
            airline_code=data["airline_code"],
            origin=data["origin"],
            destination=data["destination"],
            flight_number=data["flight_number"],
            flight_number_ext=data["flight_number_ext"],
            flight_departure_date=data["flight_departure_date"],
            departure_date=data["departure_date"],
            legs=self.create_legs(data["legs"]),
        )

    def create_legs(self, data) -> List[Leg]:
        res = []
        for leg in data:
            cabins = leg["cabins"]
            res.append(
                Leg(
                    cabins=self.create_cabins(cabins),
                    origin=leg["origin"],
                    destination=leg["destination"],
                    flight_number=leg["flight_number"],
                    flight_number_ext=leg["flight_number_ext"],
                    departure_date=leg["departure_date"],
                    arrival_date=leg["arrival_date"],
                    departure_time=leg["departure_time"],
                    arrival_time=leg["arrival_time"],
                )
            )
        self.legs = res
        return self.legs

    def create_cabins(self, data) -> List[Cabin]:
        res = []
        for cabin in data:
            classes = cabin["classes"]
            res.append(
                Cabin(
                    classes=self.create_classes(classes),
                    code=cabin["code"],
                    capacity=cabin["capacity"],
                    allotment=cabin["allotment"],
                    total_booking=cabin["total_booking"],
                    total_group_booking=cabin["total_group_booking"],
                    available_seats=cabin["available_seats"],
                    overbooking_level=cabin["overbooking_level"],
                )
            )

        self.cabins = res
        return self.cabins

    def create_classes(self, data) -> List[Class]:
        self.classes = [Class(**cls) for cls in data]
        return self.classes

    def _legs(self, **kwargs) -> List[Leg]:
        return self.data._legs(**kwargs)

    def has_legs(self, **kwargs) -> bool:
        return self.data.has_legs(**kwargs)

    @property
    def json(self):
        return self.data.json
