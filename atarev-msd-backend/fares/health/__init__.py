from dataclasses import dataclass

import pandas as pd

from configurations.repository import ConfigurationRepository
from fares.health.forms import TrackFares

repo = ConfigurationRepository()


@dataclass
class Tracker:
    all_data: pd.DataFrame
    valid_data: pd.DataFrame
    form: TrackFares
    host_carrier: str

    def track(self):
        if self.valid_data.empty:
            return [{"marketOrigin": "ALL", "marketDestination": "ALL", "carrierCode": "ALL", "validPercentage": 0}]

        merge_on = ["carrierCode", "marketOrigin", "marketDestination"]
        merged = self.all_data.merge(self.valid_data, on=merge_on, suffixes=["_all", "_valid"], how="left")
        merged["market"] = merged.apply(lambda row: f"{row.marketOrigin}-{row.marketDestination}-{row.carrierCode}", axis=1)
        result = self.__handle_markets(merged)
        result.extend(self.__handle_missing_markets(merged))

        return result

    def __handle_markets(self, merged_df: pd.DataFrame):
        merged_df["validPercentage"] = (merged_df.count_valid / merged_df.count_all) * 100
        merged_df.validPercentage = merged_df.validPercentage.fillna(0).round()
        result = merged_df[["carrierCode", "marketOrigin", "marketDestination", "validPercentage"]].to_dict("records")
        return result

    def __handle_missing_markets(self, merged_df: pd.DataFrame):
        markets = repo.get_supported_markets(self.form.get_hosts(), self.form.get_markets())

        missing_markets = merged_df[merged_df.count_valid.isna()]
        missing_markets.validPercentage.fillna(0, inplace=True)
        result = missing_markets[["carrierCode", "marketOrigin", "marketDestination", "validPercentage"]].to_dict("records")
        markets_set = set(f'{market["origin"]}-{market["destination"]}-{market["customer"]}' for market in markets)
        available_markets = merged_df.market.unique().tolist()

        for market in markets_set:
            if market not in available_markets:
                origin, destination, carrier = market.split("-")
                result.append(
                    {
                        "marketOrigin": origin,
                        "marketDestination": destination,
                        "validPercentage": 0,
                        "carrierCode": carrier,
                    }
                )

        return result
