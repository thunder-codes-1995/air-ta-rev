import datetime
import logging
import os
import time
import traceback

from dotenv import load_dotenv

from job_queue import Job, QueueManager, FareCopyTask, create_consumer_id
from job_queue.utils import add_randomness
from jobs.base_scraped_fares_processor import TripDirection
from jobs.lib.utils.logger import setup_logging
from dataclasses_serialization.bson import BSONSerializer

from jobs.scraped_fares_etl_job import ScrapedFaresProcessorFilterArguments, ExtractTransformedScrapedFares

load_dotenv()
setup_logging()
logger = logging.getLogger("fares_etl")


def perform_job(qm: QueueManager, job: Job):
    job_details: FareCopyTask = BSONSerializer.deserialize(FareCopyTask, job.payload)
    logger.debug(f"Raw job payload:{job_details}")
    # getting fares scraped after 15 minutes before the job was queued + 1 minutes for safety
    scraped_after = job.queued_at - datetime.timedelta(minutes=int(os.getenv("FARECOPY_INTERVAL", 5)), seconds=30)
    scraped_before = job.queued_at
    args = ScrapedFaresProcessorFilterArguments(host_carrier_code=job_details.host_carrier_code,
                                                scraped_after=scraped_after,
                                                scraped_before=scraped_before,
                                                origin=job_details.origin, destination=job_details.destination,
                                                direction=TripDirection.RoundTrip if job_details.direction == "RT" else TripDirection.OneWay,
                                                dry_run=False, stay_duration=job_details.stay_duration, carriers_include=job_details.carriers,)
    logger.debug(f"Converted job payload:{args}")
    ExtractTransformedScrapedFares(args).process()
    logger.info(f"Job completed without errors, completing job: {job.job_id}")
    qm.complete_job(job)


if __name__ == '__main__':
    QUEUE_POLL_INTERVAL_SECS = int(os.getenv("FARECOPY_QUEUE_POLL_INTERVAL_SECS", 3))
    CONSUMER_ID = create_consumer_id("farecopy")
    logger.info("Starting FARE COPY consumer with id: %s, queue poll interval: %s (secs)", CONSUMER_ID,
                QUEUE_POLL_INTERVAL_SECS)
    qm = QueueManager(CONSUMER_ID)

    while True:
        job = qm.get_next_fare_copy_job()
        if job is None:
            logger.info("No new job found, sleeping")
        else:
            logger.info("Got new job: %s", job.job_id)
            try:
                starting_time = time.time()
                perform_job(qm, job)
                logger.info(f"Job completed in {time.time() - starting_time} seconds")
            except Exception as e:
                logger.error("Job failed, exception: %s", str(e))
                logger.error(f"Job failed, exception: {str(e)}, stacktrace: {traceback.format_exc()}")
                qm.fail_job(job, f"Exception while performing job, exception: {str(e)}")

        time.sleep(add_randomness(QUEUE_POLL_INTERVAL_SECS, 1.5))
