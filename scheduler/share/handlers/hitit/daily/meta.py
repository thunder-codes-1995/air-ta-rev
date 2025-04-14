from typing import List


class SegmentClassRESMeta:
    @classmethod
    def columns(cls) -> List[str]:
        return [
            "airline_code",
            "flt_number",
            "flt_number_ext",
            "flt_dept_date",
            "seg_dept_date",
            "seg_origin",
            "seg_destination",
            "class",
            "cabin",
            "total_booking_class",
            "total_grp_booking_class",
            "parent_booking_class",
            "class_seats_avail",
            "class_authorizations",
            "segment_closed_indicator",
            "display_sequence",
            "market_ignore",
            "wait_list",
            "seats_sold_limit",
        ]

    @classmethod
    def name(cls) -> str:
        return "SEGCLSRES"


class LegClassRESMeta:
    @classmethod
    def columns(cls) -> List[str]:
        return [
            "airline_code",
            "flt_number",
            "flt_number_ext",
            "flt_dept_date",
            "leg_dept_date",
            "leg_origin",
            "class",
            "cabin",
            "parent_booking_class",
            "total_booking",
            "total_grp_booking",
            "class_authorization",
            "class_seats_avail",
            "wait_count",
            "display_sequence",
        ]

    @classmethod
    def name(cls) -> str:
        return "LEGCLSRES"


class FlightLegInventoryMeta:
    @classmethod
    def columns(cls) -> List[str]:
        return [
            "airline_code",
            "flt_number",
            "flt_number_ext",
            "leg_dept_date",
            "leg_origin",
            "leg_destination",
            "cabin",
            "cabin_capacity",
            "leg_index",
            "flt_dept_date",
            "leg_arrival_date",
            "leg_arrival_time",
            "leg_dept_time",
            "capacity",
            "total_grp_booking_cabin",
            "total_booking_cabin",
            "cabin_allotment",
            "wait_count",
            "cabin_level_over_booking",
            "seats_available_in_cabin",
            "aircraft_type",
        ]

    @classmethod
    def name(cls) -> str:
        return "FLTLEGINV"
