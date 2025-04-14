from dataclasses import dataclass

from core.meta import read_meta
from load_factor.meta import LoadFactorRESMeta


@dataclass
class LoadFactorSource:
    date: int
    path: str

    def __post_init__(self):
        data = read_meta(meta_class=LoadFactorRESMeta, date=self.date, path=self.path).fillna("-")

        data["origin"] = data["Leg_Board-Off"].apply(lambda val: val.split("-")[0])
        data["destination"] = data["Leg_Board-Off"].apply(lambda val: val.split("-")[1])
        data["departure_date"] = data["Leg_Dep_Date"].apply(lambda val: int("".join(val.split("/")[::-1])))
        data["Cabin_LF_Cur"] = data["Cabin_LF_Cur"].str.replace("%", "")
        data["Cabin_LF_Cur"] = data["Cabin_LF_Cur"].astype(int)
        data = data.rename(columns={"Cabin_LF_Cur": "lf", "Cabin_Code": "cabin", "Flight_Num": "flt_num"})
        data = data[["origin", "destination", "departure_date", "cabin", "lf", "flt_num"]]

        self.data = data
