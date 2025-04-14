from attr import frozen
from core.fields import number, string


@frozen(kw_only=True)
class RevenueManagmentParmas:
    airlineCode: str = string(length=2)
    arrPort: str = string(length=3, optional=True)
    depPort: str = string(length=3)
    fltNo: str = string()
    startDate: str = string()
    dayCount: str = number()


@frozen(kw_only=True)
class CabinParams:
    """Arguments needed to select a cabin from a flight"""

    leg_origin: str = string()
    leg_destination: str = string()
    cabin_code: str = string()


@frozen(kw_only=True)
class FlightParams:
    """Arguments needed to uniquely select a flight"""

    airline_code: str = string()
    origin: str = string()
    destination: str = string()
    departure_date: int = number()
    flight_number: int = number()


@frozen(kw_only=True)
class CalculateAvailabilityParams:
    """Arguments needed to uniquely select a flight"""

    cabin_name: str = string()
    action_type: str = string()
    class_rank: str = string()
    set_avail: int = number()


@frozen(kw_only=True)
class UpdateInventoryParams:
    airlineCode: str = string(length=2)
    # arrPort: str = string(length=3, optional=True)
    # depPort: str = string(length=3)
    # fltNo: str = string()
    startDate: str = string()
    dayCount: str = number()
