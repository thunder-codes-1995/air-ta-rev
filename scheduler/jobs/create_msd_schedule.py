import argparse
import datetime
import logging
import sys
from datetime import timedelta
import os
from pymongo import InsertOne

parentdir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parentdir)
from jobs.lib.utils.logger import setup_logging
from jobs.lib.utils.scraped_fares_query_utils import create_scraped_fares_match
from lib.utils.mongo_wrapper import MongoWrapper
from lib.utils.logger import setup_logging

setup_logging()
logger = logging.getLogger("msd_schedule_etl")


class MsdScheduleCreateJob:
    CHUNK_SIZE = 1000
    dry_run = False
    db = MongoWrapper()
    stats = {
        "total_inserted": 0,
    }

    def __init__(self):
        self.chunk = []

    def __get(self, lastUpdateDateTime, departureDate, orig, dest, type, source):
        """find raw scraped fares records for specified criteria(scraped date/time, origin, destination, type(OW/RT)"""
        self.reset_stats()

        aggregate = [
            {
                "$match": create_scraped_fares_match(
                    lastUpdateDateTime, departureDate, orig, dest, type, source
                )
            },
            {
                "$project": {
                    "firstItinerary": {"$arrayElemAt": ["$itineraries", 0]},
                    "cabinsNested": "$fares.itinCabins",
                }
            },
            {"$unwind": {"path": "$cabinsNested"}},
            {"$unwind": {"path": "$cabinsNested"}},
            {"$project": {"firstItinerary": 1, "cabin": "$cabinsNested.cabin"}},
            {"$unwind": {"path": "$cabin"}},
            {
                "$project": {
                    "itinOriginCode": "$firstItinerary.itinOriginCode",
                    "itinDestCode": "$firstItinerary.itinDestCode",
                    "itinDeptTime": "$firstItinerary.itinDeptTime",
                    "itinDeptDate": "$firstItinerary.itinDeptDate",
                    "legs": "$firstItinerary.legs",
                    "stops": "$firstItinerary.legs.legDestCode",
                    "carriers": "$firstItinerary.legs.mkAlCode",
                    "flightDuration": "$firstItinerary.itinDuration",
                    "cabin": 1,
                }
            },
            {
                "$group": {
                    "_id": {
                        "itinOriginCode": "$itinOriginCode",
                        "itinDestCode": "$itinDestCode",
                        "itinDeptTime": "$itinDeptTime",
                        "itinDeptDate": "$itinDeptDate",
                        "itinDuration": "$flightDuration",
                        "stops": "$stops",
                        "carriers": "$carriers",
                        "equip": "$legs.aircraft",
                        "mkFltNum": "$legs.mkFltNum",
                    },
                    "cabins": {"$addToSet": "$cabin"},
                }
            },
            {"$addFields": {"date_string": {"$toString": "$_id.itinDeptDate"}}},
            {
                "$addFields": {
                    "date_Object": {"$dateFromString": {"dateString": "$date_string"}}
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "local_dep_time": "$_id.itinDeptTime",
                    "orig_code": "$_id.itinOriginCode",
                    "dest_code": "$_id.itinDestCode",
                    "stops": "$_id.stops",
                    "carrier": "$_id.carriers",
                    "cabins": 1,
                    "duration": "$_id.itinDuration",
                    "date": {
                        "$dateToString": {"format": "%Y-%m-%d", "date": "$date_Object"}
                    },
                    "travel_year": {"$toInt": {"$substr": ["$date_string", 0, 4]}},
                    "travel_month": {"$toInt": {"$substr": ["$date_string", 4, 2]}},
                    "travel_day_of_week": {
                        "$toInt": {
                            "$dateToString": {"format": "%u", "date": "$date_Object"}
                        }
                    },
                    "equip": "$_id.equip",
                    "mkFltNum": "$_id.mkFltNum",
                }
            },
        ]

        # pipeline to count total records

        count = aggregate.copy()
        count.append({"$count": "id"})
        logger.debug(f"Query to extract fares for msd_schedule:{aggregate}")

        # count and print how many total records we will need to process
        cursor = self.db.col_scraped_fares_transformed().aggregate(count)
        for item in cursor:
            logger.info(f"Found:{item['id']} records")
        cursor = self.db.col_scraped_fares_transformed().aggregate(aggregate)
        return cursor

    def process(self, arguments):
        args = self.process_parameters(arguments)

        # delete all records in the msd_schedule before aggregation
        self.start_date = datetime.datetime.strptime(str(args.departure), "%Y%m%d")
        self.start_date = self.start_date + timedelta(days=args.offset)
        self.end_date = self.start_date + timedelta(days=args.days)

        while self.start_date < self.end_date:
            if not self.dry_run:
                delete_query = {
                    "date": self.start_date.strftime("%Y-%m-%d"),
                    "orig_code": args.origin if args.origin is not None else None,
                    "dest_code": args.destination
                    if args.destination is not None
                    else None,
                }
                logger.debug(
                    f"Delete any potential data from msd_schedule, delete query:{delete_query}"
                )
                self.db.col_msd_schedule().delete_many(delete_query)

            # get fares to be processed
            cursor = self.__get(
                args.start_datetime,
                self.start_date.strftime("%Y%m%d"),
                args.origin,
                args.destination,
                args.type,
                args.source,
            )

            # iterate over records
            for item in cursor:
                self.stats["total_processed"] += 1
                item["duration"] = int(item["duration"])
                # ignore records we dont wanna convert (e.g. ignore connecting flights, mixed operating carriers)
                if self.should_skip(item, args.carrier):
                    continue
                # process single record (normalize cabins, convert to common currency, find lowest fare, save)
                self.insert({**item})
            self.start_date += timedelta(days=1)
            # there may be some unflushed data in the buffer - make sure it's saved to mongos
            self.flush_data_buffer(False)
            # self.print_stats()
        self.flush_data_buffer(True)

    def insert(self, record):
        """insert a new record to db after proccessing it"""

        # append new object to chunk
        self.chunk.append(InsertOne(record))

        market = f'{record["orig_code"]}{record["dest_code"]}'
        if market in self.stats["markets"]:
            self.stats["markets"][market] += 1
        else:
            self.stats["markets"][market] = 0
        date = f'{record["date"]}'
        if date in self.stats["departures"]:
            self.stats["departures"][date] += 1
        else:
            self.stats["departures"][date] = 0
        # update data on chunks
        self.flush_data_buffer()

    def flush_data_buffer(self, force: bool = False):
        # print(len(self.chunk))
        if len(self.chunk) >= self.CHUNK_SIZE or (
            force == True and len(self.chunk) > 0
        ):
            col = self.db.col_msd_schedule()
            print(self.dry_run)
            if not self.dry_run:
                col.bulk_write(self.chunk)
            self.stats["total_inserted"] += len(self.chunk)
            self.print_stats()
            self.chunk = []

    def should_skip(self, record, carrier):
        """whether to skip the addition|update operation or not"""
        # if carrier filter was specified, check if it's met or not
        if carrier is not None and carrier not in record["carrier"]:
            self.stats["skipped_carrier_code_filter"] += 1
            return True

        return False

    def process_parameters(self, args):
        parser = argparse.ArgumentParser(
            description="Extract schedule from raw scraped fares (using criteria specified as arguments) and stores schedule in destination collection"
        )
        parser.add_argument(
            "start_datetime",
            help="Only records scraped after start_datetime(YYYYMMDDHHMMSS) will be taken into account",
            type=int,
        )
        parser.add_argument(
            "departure",
            help="scrape only fares for flights departing on specifed date (YYYYMMDD)",
            type=int,
        )
        parser.add_argument(
            "days", help="Number of days to scrape", type=int, default=1
        )
        parser.add_argument(
            "origin",
            help="scrape only fares for flights flying from origin airport code",
        )
        parser.add_argument(
            "destination",
            help="scrape only fares for flights flying to destination airport code",
        )
        parser.add_argument(
            "--source",
            help="filter only fares scraped from a specified website (e.g. flypgs.com, rome2rio.com)",
        )
        parser.add_argument(
            "--offset",
            help="Start date(YYYYMMDD) offset from today",
            type=int,
            default=0,
        )
        parser.add_argument(
            "--type",
            help="filter only specifed fare type (OW-One way, RT-Round trip (default)",
            default="RT",
        )
        parser.add_argument(
            "--carrier",
            help="filter only fares for specified carrier code (e.g. TK, PC). Could be comma separated values",
        )
        parser.add_argument(
            "--dryrun",
            type=bool,
            default=False,
            help="Process everything but do not save data into database",
        )
        args = parser.parse_args(args)
        self.dry_run = args.dryrun
        logger.debug(f"Parsed arguments:{args}")
        return args

    def print_stats(self):
        logger.debug(f"Stats:{self.stats}")

    def reset_stats(self):
        self.stats = {
            "total_processed": 0,
            "total_inserted": 0,
            "skipped_mixed_carrier": 0,
            "skipped_connecting": 0,
            "skipped_carrier_code_filter": 0,
            "markets": {},
            "departures": {},
        }


if __name__ == "__main__":
    args = sys.argv[1:]
    logger.info(f"Starting MSD_SCHEDULE_ETL job, arguments:{args}")
    handler = MsdScheduleCreateJob()
    handler.process(args)
