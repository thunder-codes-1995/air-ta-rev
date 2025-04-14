import argparse
import logging
import sys
import time
from abc import abstractmethod
from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
from typing import List

from jobs.lib.utils.logger import setup_logging
from jobs.lib.utils.mongo_wrapper import MongoWrapper
from jobs.lib.utils.rt_rate_handler import RoundTripRateHandler

setup_logging()
logger = logging.getLogger("fares_etl")


class TripDirection(Enum):
    OneWay = "OW"
    RoundTrip = "RT"


@dataclass(
    frozen=True,
)
class ScrapedFaresProcessorFilterArguments:
    """Class to store criteria to filter scraped fares"""

    host_carrier_code: str = None
    scraped_after: datetime = None
    scraped_before: datetime = None
    origin: str = None
    destination: str = None
    direction: TripDirection = None
    stay_duration: int = 0
    source: str = None
    carriers_include: list = None
    carriers_exclude: list = None
    max_connections: int = None
    dry_run: bool = False
    loading_id: str = None

    def __post_init__(self):
        """Validate arguments"""

        if not self.loading_id:
            if self.host_carrier_code is None or len(self.host_carrier_code) != 2:
                raise TypeError("Missing host_carrier_code argument or it is not a string of length 2")
            if self.scraped_after is None or not isinstance(self.scraped_after, datetime):
                raise TypeError("Missing scraped_after argument or it is not a datetime object")
            if self.origin is None or len(self.origin) != 3:
                raise TypeError("Missing origin argument or it is not a string of length 3")
            if self.destination is None or len(self.origin) != 3:
                raise TypeError("Missing destination argument or it is not a string of length 3")
            if self.direction is None or not isinstance(self.direction, TripDirection):
                raise TypeError("Missing direction argument or it is not a TripDirection object")
            if self.direction == TripDirection.RoundTrip and (self.stay_duration is None or self.stay_duration <= 0):
                raise TypeError("Missing or invalid stay_duration argument for round trip")

    @staticmethod
    def from_cli(arg):
        using_loading_id = "--loading_id" in sys.argv
        fields = [
            ("host_carrier_code", "get currency configs based on host", str),
            ("scraped_after", "Start date/time(YYYYMMDDHHMMSS) from which this script should take fares from", int),
            ("origin", "scrape only fares for flights flying from origin airport code", str),
            ("destination", "scrape only fares for flights flying to destination airport code", str),
            ("direction", "OW or RT (one way or round trip)", str),
            ("departure", "scrape only fares for flights departing on specified date (YYYYMMDD)", str),
            ("stay_duration", "Number of nights stay for RT fares", int),
        ]
        # parser.add_argument("host_carrier_code", help="get currency configs based on host", type=str)
        # parser.add_argument(
        #     "scraped_after", help="Start date/time(YYYYMMDDHHMMSS) from which this script should take fares from", type=int
        # )
        # parser.add_argument("origin", help="scrape only fares for flights flying from origin airport code")
        # parser.add_argument("destination", help="scrape only fares for flights flying to destination airport code")
        # parser.add_argument("direction", help="OW or RT (one way or round trip)")
        # parser.add_argument("departure", help="scrape only fares for flights departing on specified date (YYYYMMDD)")
        # parser.add_argument("stay_duration", type=int, help="Number of nights stay for RT fares")
        parser = argparse.ArgumentParser(
            description="Extract raw scraped fares (matching criteria specified as arguments) and stores processed fares in destination collection"
        )

        for field in fields:
            field_name, field_d, field_t = field
            field_name = f"--{field_name}" if using_loading_id else field_name
            parser.add_argument(field_name, help=field_d, type=field_t)

        parser.add_argument("--source", help="filter only fares scraped from a specified website (e.g. flypgs.com, rome2rio.com)")
        parser.add_argument(
            "--carriersinclude",
            help="filter only fares for specified carrier code (e.g. TK, PC). Could be comma separated values",
        )
        parser.add_argument(
            "--carriersexclude", help="ignore fares for excluded carrier code (e.g. TK, PC). Could be comma separated values"
        )
        parser.add_argument("--dryrun", type=bool, default=False, help="Process everything but do not save data into database")
        parser.add_argument("--max_connections", type=int, default=None, help="max legs count")
        parser.add_argument("--loading_id", help="handle all records holding this value in one job", type=str)

        args = parser.parse_args(arg)

        if using_loading_id:
            params = ScrapedFaresProcessorFilterArguments(loading_id=args.loading_id)
        else:
            params = ScrapedFaresProcessorFilterArguments(
                scraped_after=datetime.strptime(str(args.scraped_after), "%Y%m%d%H%M%S"),
                host_carrier_code=args.host_carrier_code,
                origin=args.origin,
                destination=args.destination,
                direction=TripDirection.OneWay if args.direction == "OW" else TripDirection.RoundTrip,
                stay_duration=args.stay_duration,
                source=args.source,
                carriers_include=args.carriersinclude.split(",") if args.carriersinclude else None,
                carriers_exclude=args.carriersexclude.split(",") if args.carriersexclude else None,
                dry_run=args.dryrun,
                max_connections=args.max_connections if args.max_connections else None,
            )
        return params


class BaseScrapedFareProcessingJob:
    """Base class for scraped fare processing jobs"""

    def __init__(self, args: ScrapedFaresProcessorFilterArguments):
        self.chunk = []
        self.args = args
        self.CHUNK_SIZE = 200
        self.db = MongoWrapper()
        self.stats = {"total_processed": 0, "total_inserted": 0, "skipped": 0}

    def create_scraped_fares_match(self):
        """Create a filter to extract only records that match specified criteria from fares collection(scraped date/time, origin, destination, type(OW/RT)"""
        filter = {
            "fares.scrapeTime": {"$gte": self.args.scraped_after, "$lt": self.args.scraped_before},
        }


        if self.args.origin is not None:
            filter["criteria.origin"] = self.args.origin

        if self.args.carriers_include is not None:
            filter["criteria.carriers"] = {"$in": self.args.carriers_include}

        if self.args.destination is not None:
            filter["criteria.destination"] = self.args.destination

        if self.args.direction is not None:
            filter["type"] = self.args.direction.value

        if self.args.source is not None:
            filter["fares.scrapedFrom"] = self.args.source

        return filter

    def create_scraped_fares_nested_match(self):
        """Each record in fares collection has multiple nested fares ('fares' property). If only certain fares should be extracted, this function will create a filter for that"""
        nested_fare_filters = {"$gte": ["$$fare.scrapeTime", self.args.scraped_after]}

        if self.args.source is not None:
            nested_fare_filters["$in"] = ["$$fare.scrapedFrom", [self.args.source]]

        # convert nested fare filters into a single condition where all items/conditions must be met
        nested_filter = {"$and": [{key: nested_fare_filters[key]} for key in nested_fare_filters]}
        return nested_filter

    def __get(self):
        """find raw scraped fares records for specified criteria(scraped date/time, origin, destination, type(OW/RT)"""
        self.reset_stats()

        main_match = self.create_scraped_fares_match()
        nested_match = self.create_scraped_fares_nested_match()
        # create aggregate pipeline
        aggregate = [
            {
                # find documents with requested origin,destination and AT LEAST one element in fares array where lastUpdateDateTime > dt
                "$match": main_match if not self.args.loading_id else {"criteria.loading_id": self.args.loading_id}
            },
            {  # filter out those elements of 'fares' array which are too old (lastUpdateDateTime <>> dt)
                "$project": {
                    "flightKey": 1,
                    "type": 1,
                    "itineraries": 1,
                    "fares": {"$filter": {"input": "$fares", "as": "fare", "cond": nested_match}},
                    "origin": "$criteria.origin",
                    "destination": "$criteria.destination",
                }
            },
        ]
        logger.debug(f"Query to fetch scraped fares:{aggregate}")
        cursor = self.get_db_wrapper().col_scraped_fares_transformed().aggregate(aggregate)
        return cursor

    def get_db_wrapper(self):
        return self.db

    def process(self):
        """Process scraped fares records"""
        s = time.time()
        cursor = self.__get()
        logger.debug(f"executing the query took {time.time() - s} seconds")
        s = time.time()
        rates = RoundTripRateHandler(self.args.host_carrier_code).rates()
        logger.debug(f"getting rates took {time.time() - s} seconds")

        # iterate over records
        for item in cursor:
            self.stats["total_processed"] += 1
            if self.should_skip(item):
                continue
            # process single record (normalize cabins, convert to common currency, find lowest fare, save)
            s = time.time()
            self.process_single_record({**item, "hostCode": self.args.host_carrier_code}, rates)
            logger.debug(f"processing single record took {time.time() - s} seconds")

        # there may be some unflushed data in the buffer - make sure it's saved to mongo
        self.flush_data_buffer(True)
        self.print_stats()

    @abstractmethod
    def process_single_record(self, item, *args):
        pass

    @abstractmethod
    def get_target_collection(self):
        """return collection where processed fares/date should be saved to"""
        pass

    def should_skip(self, record):
        include = self.args.carriers_include if self.args.carriers_include is not None else []
        exclude = self.args.carriers_exclude if self.args.carriers_exclude is not None else []
        """ whether to skip the addition|update operation or not """
        itineraries = record["itineraries"]

        if self.args.max_connections and len(itineraries) > self.args.max_connections:
            self.stats["skipped_connecting"] += 1
            return True

            # should skip if there are is more than 1 carrier
        if not self.is_single_carrier_only([itineraries[0]]):
            self.stats["skipped_mixed_carrier"] += 1
            return True

        # skip connecting flights too
        # if not self.is_direct(itineraries):
        #     self.stats['skipped_connecting'] += 1
        #     return True

        operating_carriers = self.get_carriers([itineraries[0]])
        # if carriers to be included were specified, ignore records where carrier is not in list of included carriers
        if len(include) > 0:
            for operating_carrier in operating_carriers:
                if operating_carrier not in include:
                    self.stats["skipped_carrier_code_filter"] += 1
                    return True

        # if carriers to be excluded were specified, ignore records where carrier is in list of excluded carriers
        if len(exclude) > 0:
            for operating_carrier in operating_carriers:
                if operating_carrier in exclude:
                    self.stats["skipped_carrier_code_filter"] += 1
                    return True

        return False

    def is_direct(self, itineraries) -> bool:
        """check whether a flight is direct or not"""
        is_direct = True
        for itin in itineraries:
            is_direct = len(itin["legs"]) == 1
            if not is_direct:
                break
        return is_direct

    def get_carriers(self, itineraries) -> List[str]:
        """get all unqiue carriers for an itinerary"""
        carriers = []
        for flight in itineraries:
            for leg in flight["legs"]:
                carriers.append(leg["mkAlCode"])
        return list(set(carriers))

    def is_single_carrier_only(self, itineraries) -> bool:
        """check whether a flight is operated by only one carrier"""
        return len(self.get_carriers(itineraries)) == 1

    def flush_data_buffer(self, force: bool = False):
        if len(self.chunk) >= self.CHUNK_SIZE or (force == True and len(self.chunk) > 0):
            logger.debug(f"bulk inserting {len(self.chunk)} records")
            s = time.time()
            col = self.get_target_collection()
            if not self.args.dry_run:
                col.bulk_write(self.chunk)
            logger.debug(f"bulk insert done in {time.time() - s} seconds")
            self.stats["total_inserted"] += len(self.chunk)
            self.print_stats()
            self.chunk.clear()

    def insert(self, record):
        """append record to a DB update buffer"""
        self.chunk.append(record)
        # update data on chunks
        self.flush_data_buffer()

    def print_stats(self):
        logger.debug(f"Stats:{self.stats}")

    def reset_stats(self):
        self.stats = {
            "total_processed": 0,
            "total_inserted": 0,
            "skipped_mixed_carrier": 0,
            "skipped_connecting": 0,
            "skipped_carrier_code_filter": 0,
            "skipped_not_operating": 0,
            "markets": {},
            "departures": {},
        }
