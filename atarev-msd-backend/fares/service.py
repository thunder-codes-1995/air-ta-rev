from typing import List, TypedDict

import pandas as pd
from flask import request

from airports.repository import AirportRepository
from base.entities.currency import Currency
from base.entities.exchange_rate import ExchangeRate
from base.helpers.permissions import SuperUser, has_access, has_role
from base.middlewares import attach_carriers_colors, attach_figure_id
from base.service import BaseService
from configurations.repository import ConfigurationRepository
from dds.repository import DdsRepository
from fares.availability_trends import AvTrends
from fares.availability_trends.forms import GetMinFareTrends
from fares.builder import FareBuilder
from fares.common.form import FareForm
from fares.forms import GetFareStructureTable
from fares.health import Tracker
from fares.health.forms import TrackFares
from fares.health.query import FareCountQuery
from fares.keys.data import FlightKeys
from fares.keys.forms import GetFlightKeys
from fares.price_evoluation import PE
from fares.price_evoluation.forms import GetPriceEvolution
from fares.repository import FSRepository
from fares.structure import FsTable
from fares.structure.form import GetFareStructure

config_repo = ConfigurationRepository()
dds_repo = DdsRepository()
airport_repository = AirportRepository()
builder = FareBuilder()
fs_repo = FSRepository()


class FlightKeysResp(TypedDict):
    flights: List[str]


class FareService(BaseService):

    @has_role([SuperUser])
    def get_scraper_health(self, form: TrackFares):
        valid_fare_count = self._aggregte(FareCountQuery(form, True).query)
        all_fare_couunt = self._aggregte(FareCountQuery(form, False).query)
        return Tracker(all_fare_couunt, valid_fare_count, form, request.user.carrier).track()

    @has_access("LFA", ["/availability-trends"])
    @attach_carriers_colors()
    @attach_figure_id(["fig"])
    def get_min_fare_trends(self, form: GetMinFareTrends):
        return AvTrends(form, request.user).figure()

    def get_min_fare_trends_table(self, form: GetMinFareTrends):
        return AvTrends(form, request.user).table()

    def get_price_evolution_table(self, form: GetPriceEvolution):

        sort_params = {}
        for key, value in request.args.items():
            if key.startswith("sort_"):
                column = key[len("sort_") :]
                sort_params[column] = int(value)
        return PE(form, request.user).table()

    def get_price_evolution_report(self, form: GetPriceEvolution):
        return PE(form, request.user).report()

    def get_min_fare_trends_report(self, form: GetMinFareTrends):
        return AvTrends(form, request.user).report()

    @has_access("LFA", ["/price-evolution"])
    @attach_carriers_colors()
    @attach_figure_id(["fig"])
    def get_price_evolution(self, form: GetPriceEvolution):
        return PE(form=form, user=request.user).figure()

    @has_access("MSD", ["/fare-structure"])
    @attach_carriers_colors()
    def get_fare_structure_table(self, form: GetFareStructureTable):
        fs_pipeline = builder.fare_structure_table_pipeline(form)
        fs_df = pd.DataFrame(fs_repo.aggregate(fs_pipeline))
        if fs_df.empty:
            return self.empty_figure

        resp = self.handle_fs_dfs(fs_df, form)
        return resp

    def get_min_rbkd(self, df):
        df["rbd_min_rank"] = df.groupby("rbkd")["base_fare"].rank(method="first", ascending=True)
        df = df[df["rbd_min_rank"] == 1]
        df = df.sort_values(by="total_fare_conv", ascending=True)
        return df

    def get_buyup_ratio(self, df):
        df["buyup"] = df["total_fare_conv"].diff().fillna(0)
        df["buyup_ratio"] = (df["buyup"] / df["buyup"].max()).round(2)
        return df

    def handle_fs_host_df(self, df, form):
        """get pax_ratio, min_rbkd and buyup ratio for host data"""
        df = self.get_min_rbkd(df)
        rbdk_pipline = self.builder.get_pax_rbkd_match(form)

        # get pax based on rbkd
        rbkd_pax_pax = self.lambda_df(list(self.dds_repository.get_pax_by_field("rbkd", rbdk_pipline)))

        df = (
            df.merge(rbkd_pax_pax, on="rbkd", how="left").sort_values(by="total_fare_conv", ascending=True).reset_index(drop=True)
        )

        df["pax"] = df["pax"].fillna(0)
        df["pax_ratio"] = (df["pax"] / df["pax"].sum()).round(2)
        df = self.get_buyup_ratio(df)
        return df

    def handle_fs_comp_df(self, df):
        """for each carrier get pax_ratio, min_rbkd and buyup ratio"""
        df = self.get_min_rbkd(df)
        df["pax"] = [0] * len(df)
        df["pax_ratio"] = [0] * len(df)
        df = self.get_buyup_ratio(df)
        return df

    def handle_fs_dfs(self, df, form):
        """get fare information for both host and compatitors"""
        host_fs_df = df[df["dom_op_al_code"] == request.user.carrier]
        comps_fs_dfs = df[df["dom_op_al_code"] != request.user.carrier]
        comps_data = []  # competitors data to be returned to client
        labels = self.label_data()
        host_fs_df = self.handle_fs_host_df(host_fs_df, form)
        host_fs_df.buyup = host_fs_df.buyup.astype(int)
        host_fs_df.total_fare_conv = host_fs_df.total_fare_conv.astype(int)
        host_table = self.handle_rows(host_fs_df)
        base_currency = df.currency.unique().tolist()[0]
        self.handle_fare_structure_table_currency_conversion(host_table, base_currency, form.get_currency())

        for g, comp_curr_df in comps_fs_dfs.groupby(["dom_op_al_code", "orig_code", "dest_code"]):
            # for every carrier,market:
            # - calculate min pax-rbkd (step 1)
            # - calculate buyup ratio (step 1)
            # - handle some columns (change their form in response) (step 2)
            comp_curr_df = self.handle_fs_comp_df(comp_curr_df)  # step 1
            table = self.handle_rows(comp_curr_df)  # step 2
            self.handle_fare_structure_table_currency_conversion(
                table,
                base_currency,
                form.get_currency(),
            )
            comps_data.append(
                {
                    "title": f"{g[0]}",
                    "labels": labels,
                    "data": table,
                }
            )

        return {
            "host_table": {
                "title": f"{request.user.carrier}",
                "labels": labels,
                "data": host_table,
            },
            "comp_tables": comps_data,
        }

    def handle_fare_structure_table_currency_conversion(self, data, base_currency, targeted_currency):
        if not targeted_currency:
            return
        exchange_rate = ExchangeRate(base_currency, [targeted_currency]).rates()[targeted_currency]
        currency_symbol = Currency(targeted_currency).symbol
        currency_convert_field = ["base_fare", "buyup", "total_fare_conv", "yqyr"]
        for item in data:
            for field in currency_convert_field:
                item[field] = f"{item[field] * exchange_rate:.2f}"
            item["buyup_ratio"]["text"] = item["buyup"]
            item["currency"] = currency_symbol

    def handle_rows(self, df):
        cols = df.columns.to_list()
        table = []
        for _, row in df.iterrows():
            record = self.handle_row(row, cols)
            table.append(record)
        return table

    def handle_row(self, row, cols):
        """control format for some columns in response based on existing handlers"""
        obj = {}
        # for every column :
        # if handler exists (<column_name>_handler) -> run that handler
        # otherwise get value as is
        for col in cols:
            if hasattr(self, f"{col}_handler"):
                method = getattr(self, f"{col}_handler")
                obj[col] = method(row, col)
            else:
                obj[col] = row[col]
        return obj

    def label_data(self):
        """rename data columns to be more human friendly"""

        FS_COL_CONV = {
            "pax_ratio": "PAX",
            "fare_basis": "FBC",
            "base_fare": "BASE FARE",
            "yqyr": "YQYR",
            "total_fare_conv": "ALL IN FARE",
            "currency": "CUR",
            "buyup_ratio": "BUY UPS",
            "ap": "AP",
            "min_stay": "MIN STAY",
            "non_refundable": "REF",
        }

        cols = [
            "pax_ratio",
            "fare_basis",
            "base_fare",
            "yqyr",
            "total_fare_conv",
            "non_refundable",
            "ap",
            "min_stay",
            "currency",
            "buyup_ratio",
        ]
        return [{col: FS_COL_CONV[col]} for col in cols]

    def pax_ratio_handler(self, row, col):
        """return pax ratio as dict"""
        return {"ratio": row[col], "text": str(row["pax"])}

    def buyup_ratio_handler(self, row, col):
        """return buyup as dict"""
        return {"ratio": row[col], "text": str(row["buyup"])}

    def get_flights(self, form: GetFlightKeys) -> FlightKeysResp:
        return {"flights": FlightKeys(user=request.user, form=form).get()}

    @has_access("LFA", ["/lowest-fare-calendar"])
    def get_fare_structure(self, form: GetFareStructure):
        return FsTable(form, request.user.carrier).get()
