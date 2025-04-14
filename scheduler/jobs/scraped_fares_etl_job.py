import logging
import os
import sys
from datetime import datetime, timedelta
from typing import Union

import numpy as np
import pandas as pd
from pymongo import UpdateOne

from jobs.base_scraped_fares_processor import BaseScrapedFareProcessingJob, ScrapedFaresProcessorFilterArguments
from jobs.handlers.scraped_fares_etl_handler import ScraperFaresEtlHandler
from jobs.lib.fares.fare_amount_extractor import FareAmountExtractor
from jobs.lib.fares.fare_record_converter import FareRecordConverter
from jobs.lib.utils.currency_converter import CurrencyConverter
from jobs.lib.utils.logger import setup_logging
from jobs.lib.utils.scraped_fares_query_utils import get_date_from_int
import time

setup_logging()
logger = logging.getLogger("fares_etl")
SOLD_OUT = "sold out"
AVAILABLE = "available"


class ExtractTransformedScrapedFares(BaseScrapedFareProcessingJob):
    def __init__(self, args: ScrapedFaresProcessorFilterArguments):
        super().__init__(args)
        # self.args = args
        self.currency_converter = CurrencyConverter(args.host_carrier_code)
        self.fare_record_converter = FareRecordConverter()
        self.fare_amount_extractor = FareAmountExtractor()
        self.handler = ScraperFaresEtlHandler()

    def __handle_fare(self, fare, fare_type: str, currency: str, round_trip_rate: float) -> int:
        if fare_type in fare:
            multiplied_by_rate = fare[fare_type] * round_trip_rate
            converted = self.currency_converter.convert_currency(fare["fareCurrency"], currency, multiplied_by_rate)
            return round(converted, 2)
        return 0

    def __get_fare_rate(self, item, rate_map) -> float:
        origin, destination = item["origin"], item["destination"]
        key = f"{origin}-{destination}"
        carrier = item["itineraries"][0]["legs"][0]["mkAlCode"]

        if not rate_map.get(key):
            return 1.0

        if not rate_map[key].get("carriers"):
            return rate_map[key]["rate"]

        if rate_map[key].get("carriers") and carrier in rate_map[key].get("carriers"):
            return rate_map[key]["rate"]

        return 1.0

    def process_single_record(self, item, *args):
        fares = self.fare_record_converter.convert(item["type"], item["itineraries"], item["fares"])
        round_trip_rates = args[0]
        origin, destination = item["origin"], item["destination"]
        round_trip_rate = self.__get_fare_rate(item, round_trip_rates)

        # convert currency if needed
        default_currency = self.currency_converter.get_default_currency() or "USD"
        base_currency = self.currency_converter.get_market_currency(origin, destination) or default_currency

        for fare in fares:
            fare["fareAmount"] = self.__handle_fare(fare, "fareAmount", base_currency, round_trip_rate)
            fare["baseFare"] = self.__handle_fare(fare, "baseFare", base_currency, round_trip_rate)
            fare["taxAmount"] = self.__handle_fare(fare, "taxAmount", base_currency, round_trip_rate)
            fare["yqyrAmount"] = self.__handle_fare(fare, "yqyrAmount", base_currency, round_trip_rate)
            fare["fareCurrency"] = base_currency

        # make sure to convert all scrapeTime to date object
        s = time.time()
        fares = self.handler.handle_rows(pd.DataFrame(fares))
        obj = self._create_database_record(
            {
                **item,
                "min_fares": self.get_latest_minimum_fares(fares),
                "fares": fares,
                "rt_rate": round_trip_rate,
            },
        )
        logger.debug(f"Creating database record took: {time.time() - s} seconds")

        historical_fares = obj["historical"]
        del obj["historical"]

        record = UpdateOne(
            {"flightKey": obj["flightKey"]},
            {"$set": obj, "$push": {"historicalFares": {"$each": historical_fares}}},
            upsert=True,
        )
        self.insert(record)

    def _create_database_record(self, record):
        itineraries = record["itineraries"]
        carriers = self.get_carriers(itineraries)
        historical_fares = self.handle_historical_fares(record)
        day_of_week = self.get_day_of_week(record)
        obj = {
            "hostCode": record["hostCode"],
            "flightKey": record["flightKey"],
            "carrierCode": carriers[0],
            "type": record["type"],
            "marketOrigin": itineraries[0]["itinOriginCode"],
            "marketDestination": itineraries[0]["itinDestCode"],
            "outboundDate": itineraries[0]["itinDeptDate"],
            "returnDate": itineraries[1]["itinDeptDate"] if len(itineraries) > 1 else None,
            "itineraries": itineraries,
            "insertDate": int(datetime.utcnow().strftime("%Y%m%d%H%M%S")),
            "historical": historical_fares,
            "lowestFares": record["min_fares"],
            "outboundDayOfWeek": day_of_week,
            "rt_rate": record["rt_rate"],
            "state": self.__get_flight_state(record["min_fares"]),
        }
        return obj

    def __get_flight_state(self, fare):
        last_update_time = fare[-1]["scrapeTime"]
        diff = (datetime.utcnow() - last_update_time).seconds / 3600
        if diff > int(os.getenv("SOLD_OUT_AGE")):
            return SOLD_OUT
        return AVAILABLE

    def get_day_of_week(self, record) -> int:
        date = record["itineraries"][0]["itinDeptDate"]
        date = get_date_from_int(date)
        day_of_week: int = date.weekday()
        return day_of_week

    def get_target_collection(self):
        return self.get_db_wrapper().col_fares_processed()

    def get_latest_minimum_fares(self, fares):
        """for each cabin get latest minimum fare for a flight"""

        # if no fares or mode is historical skip getting minimum fares
        if not fares:
            return []
        fares_df = pd.DataFrame(fares)

        # get latest fares
        fares_df["date"] = fares_df.scrapeTime.dt.date
        fares_df.sort_values(by="date", inplace=True, ascending=False)
        fares_df = fares_df[fares_df.date == fares_df.iloc[0]["date"]]

        # i can't groupby None (cabinName is None in some cases)
        fares_df.cabinName.fillna("unknown", inplace=True)

        # get minimum fare for each cabin
        cabin_grouped_df = fares_df.groupby(["cabinName"], as_index=False).agg({"fareAmount": "min"})

        # cabin_grouped_df has 2 columns only cabinName and fareAmount (because of grouping)
        # i need the rest of columns so i merge with the original df
        # thus i get complete data for min fares grouped by cabin name
        merged = fares_df.merge(cabin_grouped_df, on=["fareAmount", "cabinName"]).drop_duplicates(
            ["fareAmount", "cabinName"])
        columns = merged.columns.tolist()
        to_be_considered = ["travelAgency",
                            "scrapedFrom",
                            "fareAmount",
                            "fareCurrency",
                            "lastUpdateDateTime",
                            "scrapeTime",
                            "cabinName",
                            "classCode",
                            "baseFare",
                            "taxAmount",
                            "yqyrAmount", ]

        if "pointOfSale" in columns:
            to_be_considered.append("pointOfSale")

        if "fareFamily" in columns:
            to_be_considered.append("fareFamily")

        # change all unknown cabins back to None
        merged.cabinName.replace("unknown", None, inplace=True)
        merged = merged[
            to_be_considered
        ]
        return merged.to_dict("records")

    def get_dtd(self, record) -> Union[int, None]:
        """calculate days to departure"""
        if not record.get("scrapeTime") or pd.isnull(record.get("scrapeTime")):
            return None

        itineraries = record["itineraries"]
        outboundDate = itineraries[0]["itinDeptDate"] if len(itineraries) > 0 else None
        if not outboundDate:
            return None

        dt = get_date_from_int(outboundDate)
        return (dt - record.get("scrapeTime").date()).days

    def handle_historical_fares(self, record):
        now = datetime.now()
        yesterday = now - timedelta(hours=36)
        int_datetime = int(yesterday.strftime("%Y%m%d") + "000000")

        c = self.db.col_fares_processed().aggregate(
            [
                {"$match": {"flightKey": record["flightKey"]}},
                {
                    "$addFields": {
                        "historical": {
                            "$filter": {
                                "input": "$historicalFares",
                                "as": "fare",
                                "cond": {"$gte": ["$$fare.lastUpdateDateTime", int_datetime]},
                            }
                        }
                    }
                },
                {"$project": {"_id": 0, "historicalFares": 0}},
            ]
        )

        try:
            obj = next(c)
        except StopIteration:
            # new record - no previous instance
            obj = None

        fares = [{**fare, "dtd": self.get_dtd({**fare, "itineraries": record["itineraries"]})} for fare in record["fares"]]

        if not record["fares"] or not fares:
            return []

        all_fares = [*obj.get("historical", []), *fares] if obj else fares
        fares = self.remove_dupilcated_history_fares(all_fares)
        fares = self.remove_null_history_fares(fares)

        self.db.col_fares_processed().update_one(
            {"flightKey": record["flightKey"]}, {"$pull": {"historicalFares": {"lastUpdateDateTime": {"$gte": int_datetime}}}}
        )

        return fares

    def remove_dupilcated_history_fares(self, fares):
        """
        when merging already existing historical fares with newly fetched ones
        we will have duplicated records, this method will take care of that
        """
        if not fares:
            return []
        _fares = pd.DataFrame(fares)
        _fares.sort_values("scrapeTime", inplace=True, ascending=False)
        _fares.drop(["scrapeTime"], axis=1, inplace=True)
        _fares.drop_duplicates(
            ["cabinName", "fareCurrency", "classCode", "dtd", "scrapedFrom"],
            inplace=True,
        )
        _fares.replace(np.nan, None, inplace=True)
        _fares["cabinName"].fillna("Economy", inplace=True)
        return _fares.to_dict("records")

    def remove_null_history_fares(self, fares):
        if not fares:
            return []
        _fares = pd.DataFrame(fares)
        _fares = _fares[~_fares["dtd"].isnull()]
        return _fares.to_dict("records")

    def convert_fare_currency(self, record):
        """convert price from whatever currency to USD based on exchange rates"""
        fare = record["lowestFare"]
        if fare["fareCurrency"] != "USD":
            fare["fareAmount"] = self.currency_converter.convert_currency(fare["fareCurrency"], "USD", fare["fareAmount"])
        return record


if __name__ == "__main__":
    args = sys.argv[1:]
    logger.info(f"Starting FARES_ETL job, arguments:{args}")
    parsed_args = ScrapedFaresProcessorFilterArguments.from_cli(args)
    handler = ExtractTransformedScrapedFares(parsed_args)
    handler.process()
