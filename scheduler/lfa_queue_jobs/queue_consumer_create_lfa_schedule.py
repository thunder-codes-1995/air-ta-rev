import datetime
import logging
import os
import time

from dotenv import load_dotenv

from job_queue import Job, QueueManager, ExtractLFAScheduleTask, create_consumer_id
from job_queue.utils import add_randomness
from jobs.base_scraped_fares_processor import TripDirection
from jobs.create_lfa_schedule_etl_job import CreateLFAScheduleFromScrapedFares
from jobs.lib.utils.logger import setup_logging
from dataclasses_serialization.bson import BSONSerializer

from jobs.scraped_fares_etl_job import ScrapedFaresProcessorFilterArguments

load_dotenv()
setup_logging()
logger = logging.getLogger("fares_etl")


def perform_job(qm: QueueManager, job: Job):
    job_details: ExtractLFAScheduleTask = BSONSerializer.deserialize(ExtractLFAScheduleTask, job.payload)
    logger.debug(f"Raw job payload:{job_details}")
    scraped_after = job.queued_at - datetime.timedelta(minutes=int(os.getenv("FARECOPY_INTERVAL", 5)))
    scraped_before = job.queued_at
    args = ScrapedFaresProcessorFilterArguments(host_carrier_code=job_details.host_carrier_code,
                                                scraped_after=scraped_after,
                                                scraped_before=scraped_before,
                                                origin=job_details.origin, destination=job_details.destination,
                                                direction=TripDirection.RoundTrip if job_details.direction == "RT" else TripDirection.OneWay,
                                                dry_run=False, stay_duration=job_details.stay_duration)
    logger.debug(f"Converted job payload:{args}")
    CreateLFAScheduleFromScrapedFares(args).process()
    logger.info(f"Job completed without errors, completing job: {job.job_id}")
    qm.complete_job(job)


if __name__ == '__main__':
    QUEUE_POLL_INTERVAL_SECS = int(os.getenv("LFASCHEDULE_QUEUE_POLL_INTERVAL_SECS", 5))
    CONSUMER_ID = create_consumer_id("lfa_schedule_creator")
    logger.info("Starting LFA SCHEDULE EXTRACT consumer with id: %s, queue poll interval: %s (secs)", CONSUMER_ID,
                QUEUE_POLL_INTERVAL_SECS)
    qm = QueueManager(CONSUMER_ID)

    while True:
        job = qm.get_next_lfa_schedule_job()
        if job is None:
            logger.info("No new job found, sleeping")
        else:
            logger.info("Got new job: %s", job.job_id)
            try:
                perform_job(qm, job)
            except Exception as e:
                logger.error("Job failed, exception: %s", str(e))
                qm.fail_job(job, f"Exception while performing job, exception: {str(e)}")

        time.sleep(add_randomness(QUEUE_POLL_INTERVAL_SECS, 3))
