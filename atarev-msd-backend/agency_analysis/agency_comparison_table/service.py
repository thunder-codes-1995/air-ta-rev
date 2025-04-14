from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Tuple

import pandas as pd


@dataclass
class MarketShareByCarrier:
    data: pd.DataFrame
    host_code: str
    limit: int = 1

    def get(self) -> Dict[str, float]:
        """get market share for host and competitors (competitors will be limited by `limit` parameter)"""
        market_share_df: pd.DataFrame = self.data.groupby("dom_op_al_code").agg({"pax": "sum"}) / self.data.pax.sum()
        market_share_df = market_share_df.reset_index().sort_values(by="pax", ascending=False)
        others = market_share_df[market_share_df.dom_op_al_code != self.host_code].head(self.limit)
        host = market_share_df[market_share_df.dom_op_al_code == self.host_code]
        market_share_df = pd.concat([others, host])
        return {row.dom_op_al_code: row.pax for _, row in market_share_df.iterrows()}


@dataclass
class MarketShareByAgency:
    data: pd.DataFrame
    year: int
    month: int = None
    prefix: str = ""
    carriers: Iterable[str] = field(default_factory=tuple)

    def get(self) -> pd.DataFrame:
        """get market share by agency for selected carriers and specefic date"""
        targeted_df = self.__get_targeted()
        ms_by_agency_carrier = self.market_share_by_columns(targeted_df, ["agency_id", "agency_name", "dom_op_al_code"], "volume")
        ms_by_agency = self.market_share_by_columns(targeted_df, ["agency_id", "agency_name"], "market_size")
        return ms_by_agency_carrier.merge(ms_by_agency, on=["agency_id", "agency_name"]).fillna(0)

    def __get_targeted(self) -> pd.DataFrame:
        """get targeted data (by carriers and date range)"""
        query = [f"travel_year == {self.year}"]

        if self.month:
            query.append(f"travel_month == {self.month}")

        if self.carriers:
            query.append(f" dom_op_al_code in {self.carriers}")

        return self.data.query(" and ".join(query))

    def market_share_by_columns(self, targeted_df: pd.DataFrame, columns: List[str], column_name: str) -> pd.DataFrame:
        ms_df = targeted_df.groupby(columns, as_index=False).agg({"pax": "sum"})
        ms_df = ms_df.rename(columns={"pax": f"{self.prefix}_{column_name}"})
        return ms_df


@dataclass
class Growth:
    data: pd.DataFrame
    time_range: Tuple[int, int, int, int]
    growth_type: str
    host_carrier: str
    carriers: Iterable[str] = field(default_factory=tuple)

    def get(self):
        comparison_df = self.__get_comparison_df()
        comp_code = next(filter(lambda code: code != self.host_carrier, comparison_df.dom_op_al_code.unique().tolist()))
        host_growth = GrowthStats(comparison_df, self.host_carrier, self.growth_type, "host").get()
        host_growth.rename(
            columns={
                "c_volume": "current_volume",
                "p_volume": "prev_volume",
                "c_market_size": "curr_market_size",
                "p_market_size": "prev_market_size",
            },
            inplace=True,
        )

        comp_growth = GrowthStats(comparison_df, comp_code, self.growth_type, "comp").get()
        comp_growth.rename(columns={"dom_op_al_code": "comp"}, inplace=True)
        merged = host_growth.merge(comp_growth, on=["agency_id", "agency_name"])

        merged = (
            self.data[["agency_country", "agency_name", "agency_id"]]
            .merge(merged, on=["agency_name", "agency_id"])
            .drop_duplicates(["agency_id"])
        )

        return merged

    def __get_comparison_df(self) -> pd.DataFrame:
        current_year, current_month, prev_year, prev_month = self.time_range
        curr = MarketShareByAgency(self.data, current_year, current_month, "c", carriers=self.carriers).get()
        prev = MarketShareByAgency(self.data, prev_year, prev_month, "p", carriers=self.carriers).get()
        comparison_df = curr.merge(prev, on=["agency_id", "agency_name", "dom_op_al_code"])
        return comparison_df


@dataclass
class GrowthStats:
    comparison_df: pd.DataFrame
    carrier: str
    growth_type: str
    prefix: str

    def get(self):
        targeted = self.comparison_df.copy()[self.comparison_df.dom_op_al_code == self.carrier]
        if targeted.empty:
            return targeted

        growth_class = ShareGrowth if self.growth_type == "share" else VolumeGrowth
        return growth_class(targeted, self.prefix).get()


@dataclass
class ShareGrowth:
    data: pd.DataFrame
    prefix: str

    def get(self) -> pd.DataFrame:
        self.data[f"{self.prefix}_c_share"] = self.data["c_volume"] / self.data["c_market_size"] * 100
        self.data[f"{self.prefix}_p_share"] = self.data["p_volume"] / self.data["p_market_size"] * 100
        self.data[f"{self.prefix}_growth"] = self.data["c_share"] - self.data["p_share"]
        self.data[f"{self.prefix}_gap"] = (self.data["growth"] * self.data["c_market_size"] * 0.01).astype(int)
        return self.data


@dataclass
class VolumeGrowth:
    data: pd.DataFrame
    prefix: str

    def get(self) -> pd.DataFrame:
        self.data[f"{self.prefix}_c_volume"] = self.data["c_volume"] * 1
        self.data[f"{self.prefix}_p_volume"] = self.data["p_volume"] * 1
        self.data[f"{self.prefix}_growth"] = (self.data["c_volume"] - self.data["p_volume"]).astype(int)
        return self.data


@dataclass
class ComparisonTable:
    growth_df: pd.DataFrame
    growth_type: pd.DataFrame

    def get(self):
        if self.growth_type == "volume":
            return VolumeComparisonTable(self.growth_df).get()
        return ShareComparisonTable(self.growth_df).get()


@dataclass
class VolumeComparisonTable:
    growth_df: pd.DataFrame

    def get(self):
        labels = {
            "agency_id": "Agency ID",
            "agency_name": "Agency Name",
            "agency_country": "Agency Country",
            "comp": "Comp#1",
            "prev_market_size": "Market Size (TY)",
            "c_market_size": "Market Size (LY)",
            "market_growth": "Market Growth",
            "host_c_volume": "Host Vol (TY)",
            "host_p_volume": "Host Vol (LY)",
            "host_growth": "Host Vol Growth",
            "comp_c_volume": "Vol (TY)",
            "comp_p_volume": "Vol (LY)",
            "comp_growth": "Vol Growth",
        }

        return {
            "labels": labels,
            "data": self.growth_df[
                [
                    "agency_id",
                    "agency_name",
                    "agency_country",
                    "host_c_volume",
                    "host_p_volume",
                    "host_growth",
                    "comp_c_volume",
                    "comp_p_volume",
                    "comp_growth",
                    "comp",
                    "prev_market_size",
                    "c_market_size",
                ]
            ].to_dict("records"),
        }


@dataclass
class ShareComparisonTable:
    growth_df: pd.DataFrame

    def get(self):
        ...
