import dataclasses
import datetime
import json
import os
import sys
from dataclasses import dataclass
from enum import Enum
from typing import Iterable

import boto3
import certifi as certifi
from dotenv import load_dotenv
from pymongo import MongoClient

from job_queue import QueueManager, ScrapingTask

load_dotenv()


class Direction(Enum):
    OneWay = "OW"
    RoundTrip = "RT"


@dataclass(frozen=True)
class ScrapeFrequency:
    """String of 7 digits, each digit represents a day of the week, 0 is Monday, 6 is Sunday"""

    frequency_string: str

    def is_day_of_week_enabled(self, day_of_week: int) -> bool:
        return str(day_of_week) in self.frequency_string

    def __post_init__(self):
        if len(self.frequency_string) < 1:
            raise TypeError("Frequency string cannot be empty")
        for digit in self.frequency_string:
            if not digit.isdigit():
                raise TypeError(f"Frequency string must contain only digits(got {self.frequency_string})")
            if not (0 <= int(digit) <= 6):
                raise TypeError(f"Frequency string must contain only digits between 0 and 6(got {self.frequency_string}")


@dataclass(frozen=True)
class ScraperMarketSchedule:
    """
    Represents a single market schedule (start date, end date and weekly flight operating frequency)
    {
      "effectiveFrom": "2022-01-01",
      "effectiveTo": "2022-02-28",
      "frequency": "013457"
    }

    """

    effective_from: datetime.date
    effective_to: datetime.date
    frequency: ScrapeFrequency
    scrapers: list

    def is_within_effective_date_range(self, d: datetime.date) -> bool:
        """Check if a given date is within the effective date range
        :param d: date to check
        :return: True if it is, False otherwise
        """
        if self.effective_to is None:
            return self.effective_from <= d

        return self.effective_from <= d <= self.effective_to

    def __post_init__(self):
        if self.effective_from is None or self.effective_to is None:
            raise TypeError("Both effective_from and effective_to must not be empty")
        if not isinstance(self.effective_from, datetime.date):
            raise TypeError(f"Effective_from should be of date")
        if not isinstance(self.effective_to, datetime.date):
            raise TypeError("Effective_to should be of type date")

        if not self.effective_from < self.effective_to:
            raise TypeError("effective_from must be before effective_to")

        if not isinstance(self.scrapers, list) or len(self.scrapers) == 0:
            raise TypeError(f"scrapers should be a list and should not be empty")


@dataclass(frozen=True)
class ScraperMarket:
    """Object represents definition of a single market of a host carrier, with schedules, and weekly operating frequency"""

    host_carrier: str
    origin: str
    destination: str
    start_offset: int
    number_of_days_to_scrape: int
    direction: Direction
    stay_duration: int
    schedule: Iterable[ScraperMarketSchedule]

    def __post_init__(self):
        if len(self.host_carrier) != 2:
            raise TypeError(f"host_carrier looks incorrect {self.host_carrier}")
        if len(self.origin) != 3 or len(self.destination) != 3:
            raise TypeError(f"origin or destination look incorrect {self.origin}-{self.destination}")
        if type(self.start_offset) is not int or type(self.number_of_days_to_scrape) is not int:
            raise TypeError(f"start_offset and number_of_days_to_scrape must be integers")

    def _is_date_equal_or_after_start_offset(self, d: datetime) -> bool:
        """Check if a given date is equal or after the start offset"""
        return d >= datetime.date.today() + datetime.timedelta(days=self.start_offset)

    def _is_date_lower_or_equal_end_offset(self, d: datetime) -> bool:
        """check if a given date is lower or equal to the end offset"""
        return d <= datetime.date.today() + datetime.timedelta(days=(self.start_offset + self.number_of_days_to_scrape))

    def is_within_offset(self, d: datetime) -> bool:
        """Check if a given date is within the start and end offset"""
        return self._is_date_equal_or_after_start_offset(d) and self._is_date_lower_or_equal_end_offset(d)

    def is_date_operational(self, d: datetime) -> bool:
        """Check if a given date is operational for this market
        (if it is, it means there are flights available in schedule for a given date and day of week)"""
        if self.schedule is None or self.schedule.__sizeof__() == 0:
            return True

        for schedule in self.schedule:
            if schedule.is_within_effective_date_range(d):
                return schedule.frequency.is_day_of_week_enabled(d.weekday())
        return False

    def get_scrapers_to_scrape_for_date(self, d: datetime) -> list:
        """Get list of scrapers that should be scraped at a given date"""
        if self.schedule is None or self.schedule.__sizeof__() == 0:
            return []
        scrapers_for_date = []
        for schedule in self.schedule:
            if schedule.is_within_effective_date_range(d):
                for scraper in schedule.scrapers:
                    scrapers_for_date.append(scraper)
        return scrapers_for_date


class ScheduleProcessor:
    def __init__(self):
        self.db = self._initialize_mongo_client()
        self.client = boto3.client("sqs", region_name=os.getenv("AWS_REGION"))
        self.queue_url = os.getenv("SQS_QUEUE_URL")

    def generate_schedule(self, start_offset: int = 0, end_offset: int = 365, customers: list = None):
        # clear the queue
        self.client.purge_queue(QueueUrl=self.queue_url)
        scraper_markets = []
        # find all available markets of a given host carrier
        scrapable_customers = self.get_scrapable_customers()
        if customers:
            scrapable_customers = list(filter(lambda x: x in customers, scrapable_customers))

        markets = self.find_customer_markets(scrapable_customers)

        for record in markets:
            # convert mongo records to objects
            scraper_markets.append(self.convert_market_record_to_scraper_market(record["host"], record["market"]))

        # scraper_markets = self.get_scraping_dates(scraper_markets)  # calendar scraping is not ready yet

        # if it is - create a scraping task, followed by fare extraction task and followed by market+departure date reoptimization task
        today = datetime.date.today()

        for day_offset in range(start_offset, end_offset):
            for market in scraper_markets:
                for option in market.schedule:
                    for scraper in option.scrapers:
                        if day_offset <= market.number_of_days_to_scrape:
                            date = today + datetime.timedelta(day_offset)
                            scraper_configuration = scraper.get("scraperConfiguration")
                            carriers = scraper_configuration.get("carriers", [])
                            currency = scraper_configuration.get("currency", "")
                            max_stops = scraper_configuration.get("max_stops", 0)
                            max_results = scraper_configuration.get("max_results", 100)
                            self.add_market_departure_date_tasks(
                                host_carrier_code=market.host_carrier,
                                scraper=scraper["scraperName"],
                                origin=market.origin,
                                destination=market.destination,
                                departure_date=date,
                                cabin_options=[],
                                auto_transform=True,
                                stay_duration=market.stay_duration,
                                direction=market.direction,
                                carriers=carriers,
                                max_stops=max_stops,
                                max_results=max_results,
                                currency=currency,
                            )

    def add_market_departure_date_tasks(
        self,
        host_carrier_code: str,
        scraper: str,
        origin: str,
        destination: str,
        departure_date: datetime.date,
        direction: Direction,
        cabin_options=None,
        auto_transform=True,
        carriers=None,
        max_stops=0,
        max_results=10,
        stay_duration=0,
        currency="",
    ):
        departure_date_str = departure_date.strftime("%Y-%m-%d")
        task = ScrapingTask(
            host_carrier_code=host_carrier_code,
            scraper=scraper,
            origin=origin,
            destination=destination,
            departure_date=departure_date_str,
            cabin_options=cabin_options,
            auto_transform=auto_transform,
            carriers=carriers,
            max_stops=max_stops,
            max_results=max_results,
            stay_duration=stay_duration,
            direction=direction.value,
            currency=currency,
        )

        self.add_task(dataclasses.asdict(task))


    def add_task(self, message):
        response = self.client.send_message(
            QueueUrl=self.queue_url,
            MessageBody=json.dumps(message),
        )
        return response

    def find_customer_markets(self, scrapable_hosts):
        pipeline = [
            {
                "$match": {
                    "customer": {"$in": scrapable_hosts},
                    "configurationEntries.key": "MARKETS",
                }
            },
            {"$unwind": {"path": "$configurationEntries"}},
            {"$match": {"configurationEntries.key": "MARKETS"}},
            {"$unwind": {"path": "$configurationEntries.value"}},
            {"$project": {"market": "$configurationEntries.value", "host": "$customer"}},
        ]
        markets = self.db["configuration"].aggregate(pipeline)
        markets = [m for m in markets]
        return markets

    def get_scrapable_customers(self):

        pipeline = [
            {"$unwind": {"path": "$configurationEntries"}},
            {"$match": {"configurationEntries.key": "GENERATE_SCRAPING_SCHEDULE", "configurationEntries.value": True}},
            {"$project": {"host": "$customer"}},
        ]
        hosts = self.db["configuration"].aggregate(pipeline)
        hosts = [m["host"] for m in hosts]
        return hosts

    @staticmethod
    def convert_market_record_to_scraper_market(host_carrier_code: str, market) -> ScraperMarket:
        schedules = []
        if market["schedule"] is not None:
            for schedule in market["schedule"]:
                schedules.append(
                    ScraperMarketSchedule(
                        effective_from=schedule["effectiveFrom"].date(),
                        effective_to=schedule["effectiveTo"].date(),
                        frequency=ScrapeFrequency(frequency_string=schedule["frequency"]),
                        scrapers=schedule["scrapers"],
                    )
                )
        return ScraperMarket(
            host_carrier=host_carrier_code,
            origin=market["orig"],
            destination=market["dest"],
            start_offset=market["startOffset"],
            number_of_days_to_scrape=market["numberOfDays"],
            direction=Direction(market["direction"]),
            stay_duration=market["stayDuration"],
            schedule=schedules,
        )

    def _initialize_mongo_client(self):
        connection_string = (
            f'mongodb+srv://{os.getenv("DB_USER")}:{os.getenv("DB_PASSWORD")}@{os.getenv("DB_HOST")}/?retryWrites=true&w=majority'
        )
        mongo_client = MongoClient(connection_string, tlsCAFile=certifi.where())
        db = mongo_client[os.getenv("DB_NAME")]
        return db


if __name__ == "__main__":
    # FIXME: get list of markets from the DB
    print(sys.argv)
    start_offset = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    end_offset = int(sys.argv[2]) if len(sys.argv) > 2 else 33
    customers = sys.argv[3].split(",") if len(sys.argv) > 3 else []
    ScheduleProcessor().generate_schedule(start_offset, end_offset, customers)
