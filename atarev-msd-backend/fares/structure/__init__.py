from dataclasses import dataclass

import pandas as pd

from base.helpers.cabin import CabinMapper
from base.helpers.datetime import Date
from configurations.service import ConfigurationService
from fares.repository import FareRepository, FareStructureRepository
from fares.structure.form import GetFareStructure
from fares.structure.query import FareBreakdownQuery

fs_repo = FareStructureRepository()
fares_repo = FareRepository()


@dataclass
class FsTable:
    form: GetFareStructure
    host_code: str

    def get(self):
        labels = {
            "cabin": "CABIN",
            "origin": "ORG",
            "destination": "DST",
            "fare_basis": "FBC",
            "base": "BASE",
            "q": "Q",
            "yq": "YQ",
            "yr": "YR",
            "tax": "Taxes",
            "all_in": "ALL IN",
            "currency": "CUR",
            "q_currency": "Q CUR",
            "yq_currency": "YQ CUR",
            "yr_currency": "YR CUR",
            "last_updated_date": "Last Upd Dte",
        }

        fs = FS(self.form, self.host_code).get()
        breakdown = FareBreakdown(self.form, self.host_code).get()

        merged = fs.merge(
            breakdown[["origin", "destination", "norm_cabin", "class", "dept_date", "tax"]],
            left_on=("origin", "destination", "cabin", "class", "date"),
            right_on=("origin", "destination", "norm_cabin", "class", "dept_date"),
            how="left",
        )

        if merged.empty:
            return {"table": [], "labels": labels}

        merged.tax.fillna(0, inplace=True)
        merged["all_in"] = merged.apply(lambda row: row.base + row.q + row.yq + row.yr + row.surcharge + row.tax, axis=1)
        merged["last_updated_date"] = merged["date"].apply(lambda val: Date(val).humanize())

        return {"table": merged[list(labels.keys())].to_dict("records"), "labels": labels}


@dataclass
class FS:
    form: GetFareStructure
    host_code: str

    def get(self):
        to_be_considered_fare_basis = ConfigurationService().get_by_key("FARE_BASIS")

        fs = pd.DataFrame(
            list(
                fs_repo.get_fare_structure(
                    carrier_code=self.host_code,
                    origin=self.form.get_origin(),
                    destination=self.form.get_destination(),
                    cabins=self.form.get_cabin(),
                    fare_basis=to_be_considered_fare_basis.get("value"),
                )
            )
        )

        if fs.empty:
            return pd.DataFrame(
                columns=[
                    "cabin",
                    "destination",
                    "origin",
                    "class",
                    "date",
                    "fare_basis",
                    "base",
                    "q",
                    "yq",
                    "yr",
                    "currency",
                    "q_currency",
                    "yq_currency",
                    "yr_currency",
                    "surcharge",
                ]
            )

        fs.q.replace("-", 0, inplace=True)
        fs.yq.replace("-", 0, inplace=True)
        fs.yr.replace("-", 0, inplace=True)
        fs.base.replace("-", 0, inplace=True)
        fs.surcharge.replace("-", 0, inplace=True)
        fs.sort_values("date", ascending=False, inplace=True)
        fs.drop_duplicates(["origin", "destination", "cabin", "fare_basis"], inplace=True)
        return fs


@dataclass
class FareBreakdown:
    form: GetFareStructure
    host_code: str

    def get(self):
        match = FareBreakdownQuery(self.form).query
        fares_breakdown = pd.DataFrame(fares_repo.get_fares_breakdown({**match, "hostCode": self.host_code}))

        if fares_breakdown.empty:
            return pd.DataFrame(
                columns=[
                    "origin",
                    "destination",
                    "norm_cabin",
                    "class",
                    "dept_date",
                    "tax",
                ]
            )

        fares_breakdown["norm_cabin"] = fares_breakdown.cabin.apply(lambda c: CabinMapper.normalize(c))
        return fares_breakdown
