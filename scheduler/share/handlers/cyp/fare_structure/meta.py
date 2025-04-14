from typing import List


class FareStructureRESMeta:
    @classmethod
    def columns(cls) -> List[str]:
        return [
            "origin",
            "destination",
            "fare_basis",
            "ft_datetime",
            "lt_datetime",
            "min_stay",
            "t_dow",
            "first_ticketing_datetime",
            "last_ticketing_datetime",
            "cabin",
            "class",
            "advance_purchase_days",
            "trip_type",
            "curr",
            "base_fare",
            "surcharge_amt",
            "q",
            "q_curr",
            "yq",
            "yq_curr",
            "yr",
            "yr_curr",
            "pos_country",
            "fare_family",
            "footnote",
            "observed_at",
            "rtg",
        ]

    @classmethod
    def name(cls) -> str:
        return "filed_fares"
