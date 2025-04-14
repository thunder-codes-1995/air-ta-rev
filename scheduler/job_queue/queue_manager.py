import datetime
import logging
import os
from abc import abstractmethod
from dataclasses import dataclass
from uuid import uuid4

import bson as bson
import certifi as certifi
from dataclasses_serialization.bson import BSONSerializer
from dotenv import load_dotenv
from jobs.lib.utils.logger import setup_logging
from pymongo import MongoClient

from job_queue.mongo_queue.job import Job
from job_queue.mongo_queue.queue import Queue

load_dotenv()
setup_logging()
logger = logging.getLogger("queue_manager")


@dataclass(frozen=True)
class AbstractQueueTask:
    host_carrier_code: str

    @abstractmethod
    def task_id(self) -> str:
        pass

    @abstractmethod
    def job_type(self) -> str:
        pass


@dataclass(frozen=True)
class BaseAbstractTask(AbstractQueueTask):
    origin: str
    destination: str
    cabin_options: list
    auto_transform: bool
    carriers: list
    max_stops: int
    max_results: int
    stay_duration: int
    direction: str

    @abstractmethod
    def task_id(self) -> str:
        return f"{self.host_carrier_code}/{self.origin}{self.destination}/{self.stay_duration}/{self.job_type()}/{str(uuid4())}"

    @abstractmethod
    def job_type(self) -> str:
        pass


@dataclass(frozen=True)
class ScrapingTask(BaseAbstractTask):
    departure_date: str
    """Task to scrape a given market/date using specified scraper"""

    scraper: str
    currency: str

    def task_id(self) -> str:
        return f"{self.host_carrier_code}/{self.origin}{self.destination}/{self.departure_date}/{self.stay_duration}/{self.job_type()}/{self.scraper}/{str(uuid4())}"

    def job_type(self) -> str:
        return "SCRAPE"


@dataclass(frozen=True)
class CalendarScrapingTask:
    """Task to scrape a given market/date using specified scraper"""

    origin: str
    destination: str
    stay_duration: int
    calendar_start_date: str
    calendar_end_date: str
    carrier: str

    def task_id(self) -> str:
        return f"{self.origin}{self.destination}/{self.stay_duration}/{self.carrier}/{self.job_type()}/{str(uuid4())}"

    def job_type(self) -> str:
        return "CALENDAR_SCRAPE"


@dataclass(frozen=True)
class MarketReoptimizationTask(BaseAbstractTask):
    """Task to optimize given market/date"""

    def job_type(self) -> str:
        return "REOPTIMIZE"


@dataclass(frozen=True)
class AuthorizationCalculationTask(AbstractQueueTask):
    """Task to calculate authorization"""

    origin: str
    destination: str
    departure_date: str
    flight_number: int
    carrier_code: str
    cabin_name: str
    action_type: str
    class_rank: str
    set_avail: int
    rule_result_id: bson.ObjectId

    def task_id(self) -> str:
        return f"{self.origin}{self.destination}/{self.departure_date}/{self.carrier_code}{self.flight_number}/{self.job_type()}/{str(uuid4())}"

    def job_type(self):
        return "CALCULATE_AUTH"


class FareCopyTask(BaseAbstractTask):
    def job_type(self) -> str:
        return "COPYFARES"


class ExtractLFAScheduleTask(BaseAbstractTask):
    def job_type(self) -> str:
        return "EXTRACT_LFA_SCHEDULE"


class QueueManager:
    TOPICS = {  # TODO convert to ENUM
        "SCRAPE": "scrape-topic",
        "CALENDAR_SCRAPE": "calendar-topic",
        "REOPT": "reopt-topic",
        "COPYFARES": "copyfares-topic",
        "EXTRACT_LFA_SCHEDULE": "lfaschedule-topic",
        "DEFAULT": "default-topic",
        "CALCULATE_AUTH": "calculate-authorization",
    }

    def __init__(self, consumer_id: str = f"consumer{str(uuid4())}"):
        connection_string = (
            f'mongodb+srv://{os.getenv("DB_USER")}:{os.getenv("DB_PASSWORD")}@{os.getenv("DB_HOST")}/?retryWrites=true&w=majority'
        )
        mongo_client = MongoClient(connection_string, tlsCAFile=certifi.where())
        db = mongo_client[os.getenv("DB_NAME")]
        col = db.scrapers_task_queue
        logger.info(f"Initialized QueueManager with consumer_id: {consumer_id}")
        self.queue = Queue(col, consumer_id=consumer_id, timeout=300, max_attempts=3)

    def _get_next_job(self, topic: str = "DEFAULT") -> Job:
        channel = QueueManager.TOPICS[topic]
        return self.queue.next(channel)

    def get_next_scrape_job(self) -> Job:
        return self._get_next_job("SCRAPE")

    def get_next_reopt_job(self) -> Job:
        return self._get_next_job("REOPT")

    def get_next_auth_calc_job(self) -> Job:
        return self._get_next_job("CALCULATE_AUTH")

    def get_next_fare_copy_job(self) -> Job:
        return self._get_next_job("COPYFARES")

    def get_next_lfa_schedule_job(self) -> Job:
        return self._get_next_job("EXTRACT_LFA_SCHEDULE")

    def _add_job(self, job: BaseAbstractTask, depends_on=[], topic: str = "DEFAULT"):
        channel = QueueManager.TOPICS[topic]
        return self.queue.put(BSONSerializer.serialize(job), job_id=job.task_id(), channel=channel, depends_on=depends_on)

    def add_scrape_job(self, task: ScrapingTask, depends_on=[]) -> Job:
        return self._add_job(task, depends_on, "SCRAPE")

    def add_calendar_scrape_job(self, task: CalendarScrapingTask, depends_on=[]) -> Job:
        return self._add_job(task, depends_on, "CALENDAR_SCRAPE")

    def add_fare_copy_job(self, task: FareCopyTask, depends_on=[]) -> Job:
        return self._add_job(task, depends_on, "COPYFARES")

    def add_reopt_task(self, task: MarketReoptimizationTask, depends_on=[]) -> Job:
        return self._add_job(task, depends_on, "REOPT")

    def add_auth_calc_task(self, task: AuthorizationCalculationTask, depends_on=[]) -> Job:
        return self._add_job(task, depends_on, "CALCULATE_AUTH")

    def add_lfs_schedule_task(self, task: ExtractLFAScheduleTask, depends_on=[]) -> Job:
        return self._add_job(task, depends_on, "EXTRACT_LFA_SCHEDULE")

    def complete_job(self, job: Job):
        logger.info(f"Completing job with id: {job.job_id}")
        job.complete()

    def fail_job(self, job: Job, error_message: str):
        logger.warn(f"Failing job: {job} with id: {job.job_id}, error: {error_message}")
        job.error(error_message)

    def clear(self):
        self.queue.clear()
