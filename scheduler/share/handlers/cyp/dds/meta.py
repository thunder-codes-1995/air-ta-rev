from typing import List


class DDsRESMeta:
    @classmethod
    def columns(cls) -> List[str]:
        return [
            "leg_flt_num",
            "dept_date",
            "bkng_date",
            "dept_month",
            "class",
            "origin",
            "destination",
            "fare_basis",
            "mk_airline_code",
            "mk_flt_num",
            "op_airline_code",
            "pnr_itin",
            "creator_iata",
            "city_pos",
            "creator_officee",
            "issuance_iata",
            "creator_gds_code",
            "pos_gds_code",
            "path",
            "bkn_month",
            "bkn_year",
            "cabin",
            "aircraft_type",
            "dept_datetime",
            "pax",
            "rev",
            "lf",
            "avg_fare",
            "cap",
        ]

    @classmethod
    def name(cls) -> str:
        return "dds"
