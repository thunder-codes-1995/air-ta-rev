import logging
import os
import time
from typing import Literal

import requests
from dataclasses_serialization.bson import BSONSerializer
from dotenv import load_dotenv
from jobs.lib.utils.logger import setup_logging

from job_queue import Job, MarketReoptimizationTask, QueueManager, create_consumer_id
from job_queue.utils import add_randomness
from datetime import timedelta

load_dotenv()
setup_logging()
logger = logging.getLogger("fares_etl")


def evaluate(
    host_carrier_code: str,
    origin: str,
    destination: str,
    start_datetime: int,
    end_datetime: int,
    cabin: Literal["Y", "J"],
) -> requests.Response:

    url = f"{os.getenv('REOPT_LFA_URL')}"
    response = requests.post(
        url,
        json={
            "origin": origin,
            "destination": destination,
            "host_code": host_carrier_code,
            "start_datetime": start_datetime,
            "end_datetime": end_datetime,
            "cabin": cabin,
        },
    )

    return response


def perform_job(qm: QueueManager, job: Job):
    job_details: MarketReoptimizationTask = BSONSerializer.deserialize(MarketReoptimizationTask, job.payload)
    logger.debug(f"Raw job payload:{job_details}")
    host_carrier_code = job_details.host_carrier_code
    origin = job_details.origin
    destination = job_details.destination
    star_datetime = int((job.queued_at - timedelta(minutes=int(os.getenv("FARECOPY_INTERVAL", 5)), seconds=30)).strftime("%Y%m%d%H%M%S"))
    end_datetime = int(job.queued_at.strftime("%Y%m%d%H%M%S"))
    logger.debug(f"Will reopt at host:{host_carrier_code}, market: {origin}-{destination} start_datetime {star_datetime} end_datetime {end_datetime}")
    cabins = job_details.cabin_options or ["Y", "J", "F"]

    try:
        for cabin in cabins:
            response = evaluate(
                origin=origin,
                destination=destination,
                host_carrier_code=host_carrier_code,
                start_datetime=star_datetime,
                end_datetime=end_datetime,
                cabin=cabin,
            )

        if response.ok:
            logger.info(f"Job completed without errors, completing job: {job.job_id}")
            qm.complete_job(job)
        else:
            raise ValueError(f"evaluation for cabin code {cabin} has failed")

    except ValueError:
        qm.fail_job(job, f"Reopt failed with status code: {response.status_code}")


if __name__ == "__main__":

    QUEUE_POLL_INTERVAL_SECS = int(os.getenv("REOPT_QUEUE_POLL_INTERVAL_SECS", 5))
    CONSUMER_ID = create_consumer_id("reopt")
    logger.info("Starting REOPT consumer with id: %s, queue poll interval: %s (secs)", CONSUMER_ID, QUEUE_POLL_INTERVAL_SECS)
    qm = QueueManager(CONSUMER_ID)

    while True:
        job = qm.get_next_reopt_job()
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
