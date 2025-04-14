import os
import random
import time

from dotenv import load_dotenv

from job_queue import Job,  QueueManager, ScrapingTask
from dataclasses_serialization.bson import BSONSerializer

load_dotenv()

poll_interval = os.getenv("SCRAPER_QUEUE_POLL_INTERVAL_SECS", 5)


def perform_job(qm: QueueManager, job: Job):
    print("Performing SCRAPE job: ", job)
    details = BSONSerializer.deserialize(ScrapingTask, job.payload)
    print("Payload: ", details)
    if random.randint(0, 10) < 5:
        qm.fail_job(job, "test")
    else:
        qm.complete_job(job)


if __name__ == '__main__':
    qm = QueueManager("scraper1")
    while True:
        job = qm.get_next_scrape_job()
        if job is None:
            print("No job found, sleeping")
            time.sleep(3)
        else:
            perform_job(qm, job)
            print("Sleeping")
            time.sleep(3)
            print("Waking up")
