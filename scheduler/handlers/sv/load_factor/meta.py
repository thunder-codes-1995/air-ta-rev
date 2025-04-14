from typing import List


class LoadFactorRESMeta:
    @classmethod
    def columns(cls) -> List[str]:
        return [
            "Leg_Board-Off",
            "Leg_Dep_Date",
            "Flight_Num",
            "Cabin_Code",
            "Cabin_LF_Cur",
        ]

    @classmethod
    def name(cls) -> str:
        return "lf"
