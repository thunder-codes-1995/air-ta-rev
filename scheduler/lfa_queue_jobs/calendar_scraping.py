import datetime
import os
import certifi as certifi
from dotenv import load_dotenv
from pymongo import MongoClient

from job_queue import QueueManager
from job_queue.queue_manager import CalendarScrapingTask
from lfa_queue_jobs.schedule_processor import ScheduleProcessor

load_dotenv()


class CalendarProcessor:
    def __init__(self):
        self.db = self._initialize_mongo_client()
        self.queue_manager = QueueManager("calendar-processor")

    def generate_schedule(self, days_to_scrap=120):
        self.queue_manager.clear()
        scraper_markets = []
        # find all available markets of a given host carrier
        markets = self.find_customer_markets()
        for record in markets:
            # convert mongo records to objects
            scraper_markets.append(
                ScheduleProcessor.convert_market_record_to_scraper_market(record['host'], record['market']))
        calendar_start_date = datetime.datetime.utcnow()
        calendar_end_date = calendar_start_date + datetime.timedelta(days=days_to_scrap)
        for market in scraper_markets:
            for option in market.schedule:
                for scraper in option.scrapers:
                    for carrier in scraper['scraperConfiguration']['carriers']:
                        task = CalendarScrapingTask(carrier=carrier, origin=market.origin,
                                                    destination=market.destination,
                                                    stay_duration=market.stay_duration,
                                                    calendar_start_date=calendar_start_date.strftime("%Y-%m-%d"),
                                                    calendar_end_date=calendar_end_date.strftime("%Y-%m-%d"))
                        self.queue_manager.add_calendar_scrape_job(task=task, depends_on=[])

    def _initialize_mongo_client(self):
        connection_string = f'mongodb+srv://{os.getenv("DB_USER")}:{os.getenv("DB_PASSWORD")}@{os.getenv("DB_HOST")}/?retryWrites=true&w=majority'
        mongo_client = MongoClient(connection_string, tlsCAFile=certifi.where())
        db = mongo_client[os.getenv("DB_NAME")]
        return db

    def find_customer_markets(self):
        pipeline = [
            {
                '$match': {
                    'configurationEntries.key': 'MARKETS'
                }
            }, {
                '$unwind': {
                    'path': '$configurationEntries'
                }
            }, {
                '$match': {
                    'configurationEntries.key': 'MARKETS'
                }
            }, {
                '$unwind': {
                    'path': '$configurationEntries.value'
                }
            }, {
                '$project': {
                    'market': '$configurationEntries.value',
                    'host': '$customer'
                }
            }
        ]
        markets = self.db["configuration"].aggregate(pipeline)
        markets = [m for m in markets]
        return markets


if __name__ == '__main__':
    CalendarProcessor().generate_schedule()
