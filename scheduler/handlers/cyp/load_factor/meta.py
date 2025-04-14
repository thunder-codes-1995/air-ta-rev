from typing import List


class LoadFactorRESMeta:
    @classmethod
    def columns(cls) -> List[str]:
        return [
            "dow",
            "dept_date",
            "carrier",
            "flt_num",
            "dept_time",
            "origin",
            "destination",
            "equipment",
            "cs",
            "lf_bus",
            "lf_eco",
            # "adj",
            "capacity_j",
            "booked_j",
            "booked_grp_j",
            "capacity_y",
            "booked_y",
            "booked_grp_y",
        ]

    @classmethod
    def name(cls) -> str:
        return "lf"
