import concurrent.futures
import logging
import os
import random
import time
from datetime import date, timedelta, datetime

import requests as requests
from dotenv import load_dotenv

from lib.utils.logger import setup_logging

load_dotenv()

# how many concurrent scrapers should run on a single server
SCRAPER_ENABLED = os.getenv('SCRAPER_SCRAPING_ENABLED', default="False")
if SCRAPER_ENABLED != "True":
    exit("Scraping is disabled")

# how many concurrent scrapers should run on a single server
JOBS_PER_SERVER = int(os.getenv('SCRAPER_JOBS_PER_SERVER', default=1))
# get list of servers
SERVERS = os.getenv('SCRAPER_SERVERS') if os.getenv('SCRAPER_SERVERS') is not None else ''
SERVERS = SERVERS.split(',')
if len(SERVERS) == 0:
    raise Exception('env variable SCRAPER_SERVERS is not set')

# how many days into the future we should scrape?
NUMBER_OF_DAYS_TO_SCRAPE = int(os.getenv('SCRAPER_NUMBER_OF_DAYS_TO_SCRAPE', default=30))
# should we start from today or offset?
START_OFFSET_IN_DAYS = int(os.getenv('SCRAPER_START_OFFSET_IN_DAYS', default=0))

print(
    f"NUMBER_OF_DAYS_TO_SCRAPE={NUMBER_OF_DAYS_TO_SCRAPE}, START_OFFSET_IN_DAYS={START_OFFSET_IN_DAYS},  Jobs per server:{JOBS_PER_SERVER}, Servers:{SERVERS},")
DRY_RUN = False



MARKETS = [
        {
            "origin": "PBM",
            "destination": "AMS",
            "ret": 14,
            "scrapers": ["slm", "itasoftware"]
        },
        {
            "origin": "AMS",
            "destination": "PBM",
            "ret": 14,
            "scrapers": ["slm", "itasoftware"]
        }]


if os.getenv('ENV_NAME') == 'production':
    MARKETS = [
        {
            "origin": "PBM",
            "destination": "AMS",
            "ret": 14,
            "scrapers": ["slm", "itasoftware"]
        },
        {
            "origin": "AMS",
            "destination": "PBM",
            "ret": 14,
            "scrapers": ["slm", "itasoftware"]
        }]

elif os.getenv('ENV_NAME') == 'dev':
    MARKETS = [
        {
            "origin": "PBM",
            "destination": "AMS",
            "ret": 14,
            "scrapers": ["slm", "itasoftware"]
        },
        # {
        #     "origin": "AMS",
        #     "destination": "PBM",
        #     "ret": 14,
        #     "scrapers": ["slm", "itasoftware"]
        # },

        {
            "origin": "PBM",
            "destination": "POS",
            "scrapers": ["slm", "itasoftware"]  # add ita
        },
        # {
        #     "origin": "POS",
        #     "destination": "PBM",
        #     "scrapers": ["slm", "itasoftware"]  # add ita
        # },

        {
            "origin": "YLW",
            "destination": "YYJ",
            "ret": 7,
            "scrapers": ["pasco","itasoftware"]
        },
        # {
        #     "origin": "YYJ",
        #     "destination": "YLW",
        #     "ret": 7,
        #     "scrapers": ["pasco", "itasoftware"]
        # },

        {
            "origin": "LCA",
            "destination": "ATH",
            "ret": 7,
            "scrapers": ["itasoftware"]
        },
        # {
        #     "origin": "ATH",
        #     "destination": "LCA",
        #     "ret": 7,
        #     "scrapers": ["itasoftware"]
        # },
        #
        {
            "origin": "BEY",
            "destination": "LCA",
            "ret": 7,
            "scrapers": ["itasoftware"]
        },
        # {
        #     "origin": "LCA",
        #     "destination": "BEY",
        #     "ret": 7,
        #     "scrapers": ["itasoftware"]
        # },
        #
        {
            "origin": "LCA",
            "destination": "TLV",
            "ret": 7,
            "scrapers": ["itasoftware"]
        },
        # {
        #     "origin": "TLV",
        #     "destination": "LCA",
        #     "ret": 7,
        #     "scrapers": ["itasoftware"]
        # },


    ]

elif os.getenv('ENV_NAME') == 'demo':
    MARKETS = [{
            "origin": "YLW",
            "destination": "YYJ",
            "ret": 7,
            "scrapers": ["itasoftware"]
        },
        {
            "origin": "YYJ",
            "destination": "YLW",
            "ret": 7,
            "scrapers": ["itasoftware"]
        }]


        # {
        # "YYJ-YLW": {
        #     "ret": 7,
        #     "scrapers": [
        #         "pasco", "itasoftware"
        #     ]
        # },
        # "YLW-YYJ": {
        #     "ret": 7,
        #     "scrapers": [
        #         "pasco", "itasoftware"
        #     ]
        # },
        # }

# sleep between scrapings
SLEEP_TIME_IN_SECONDS = 5

setup_logging()
logging = logging.getLogger('scraper_job')

statistic_dict = {}


def scrape_day_using_scraper(BASE_URL, orig, dest, dptr, ret, scraper):
    url = f"{BASE_URL}/scraper/{scraper}?origin={orig}&destination={dest}&departure={dptr}&return={ret}"
    logging.debug(f"Scraping {orig}-{dest} {dptr}/{ret}, url:{url},")
    start_dt = time.time()
    if DRY_RUN != True:
        response = requests.get(url)
        status = response.content
    else:
        status = b"OK"

    logging.debug(f"STATS: Scraper:{scraper}, scraping result = {status}, time = {time.time() - start_dt}")
    return status == b"OK"


def scrape_job(BASE_URL, orig, dest, dptr_int, return_int, scraper, job_queue_index, job_queue_size):
    logging.debug(
        f"{job_queue_index}/{job_queue_size} Start scraping dept_date={dptr_int}, return={return_int}, market:{orig}-{dest}, BASE_URL={BASE_URL}")
    time.sleep(SLEEP_TIME_IN_SECONDS)
    stats = {}
    logging.info(f"Scrape {dptr_int} with scraper:{scraper}")
    try:
        if BASE_URL not in statistic_dict.keys():
            statistic_dict[BASE_URL] = {"total_number_website_to_be_scraped": 0}
        statistic_dict[BASE_URL]["total_number_website_to_be_scraped"] += 1

        if f"total_{scraper}_to_be_scraped" not in statistic_dict[BASE_URL].keys():
            statistic_dict[BASE_URL][f"total_{scraper}_to_be_scraped"] = 0
        statistic_dict[BASE_URL][f"total_{scraper}_to_be_scraped"] += 1

        scrape_start_time = time.time()
        result = scrape_day_using_scraper(BASE_URL, orig, dest, dptr_int, return_int, scraper)
        scrape_time = time.time() - scrape_start_time
        stats['scrape_time'] = int(scrape_time)
        stats['scrape_result'] = result
        # print(f"\t\t Scrape {orig}-{dest} {dptr_int}/{return_int} {scraper}, result:{result}")

        if False == result:
            logging.error(f"\t\tFailed to scrape {orig}-{dest} {dptr_int}/{return_int}, using scraper:{scraper}")
            if f"total_{scraper}_Failed" not in statistic_dict[BASE_URL].keys():
                statistic_dict[BASE_URL][f"total_{scraper}_Failed"] = 0
            statistic_dict[BASE_URL][f"total_{scraper}_Failed"] += 1
            if f"total_number_website_failed" not in statistic_dict[BASE_URL].keys():
                statistic_dict[BASE_URL][f"total_number_website_failed"] = 0
            statistic_dict[BASE_URL][f"total_number_website_failed"] += 1
        else:
            if f"total_{scraper}_Successed" not in statistic_dict[BASE_URL].keys():
                statistic_dict[BASE_URL][f"total_{scraper}_Successed"] = 0
            statistic_dict[BASE_URL][f"total_{scraper}_Successed"] += 1

            if f"total_number_websites_Successed" not in statistic_dict[BASE_URL].keys():
                statistic_dict[BASE_URL][f"total_number_website_Successed"] = 0
            statistic_dict[BASE_URL][f"total_number_website_Successed"] += 1

        if (job_queue_index % 10 == 0):
            logging.warning(f"statistics: {statistic_dict}")

    except Exception as e:
        logging.error(
            f"\t\tException while scraping {orig}-{dest} {dptr_int}/{return_int}, using scraper:{scraper}, BASE_URL={BASE_URL}, got exception:{e}")
        stats['scrape_time'] = 0
        stats['scrape_result'] = 'exception'
        if statistic_dict[BASE_URL][f"total_{scraper}_Failed_with_errors"] not in statistic_dict[BASE_URL].keys():
            statistic_dict[BASE_URL][f"total_{scraper}_Failed_with_errors"] = 0
        statistic_dict[BASE_URL][f"total_{scraper}_Failed_with_errors"] += 1
        if f"total_number_website_failed_with_errors" not in statistic_dict[BASE_URL].keys():
            statistic_dict[BASE_URL][f"total_number_website_failed_with_errors"] = 0
        statistic_dict[BASE_URL][f"total_number_website_failed_with_errors"] += 1

    stats_str = f"{scraper},{stats['scrape_time']},{stats['scrape_result']}"
    logging.info(
        f"STATS: {job_queue_index}/{job_queue_size} Scraping stats,{orig}-{dest},{dptr_int},{return_int},{stats_str}, {BASE_URL}")


class SingleScrapingJob:
    base_url: str = ''
    dept_date: date = 0
    return_date: date = None
    origin: str = ''
    destination: str = ''
    scraper: str = ''

    def __repr__(self) -> str:
        return f"{self.origin}-{self.destination} {self.dept_date} {self.return_date} by {self.scraper}"


def create_scrape_jobs(servers, markets, strategy="date_market"):
    print(random.randint(0, len(servers) - 1))
    jobs_queue = []
    if strategy == "market_date":# FIXME this will not work
        for market in markets.keys():
            start_date = datetime.utcnow() + timedelta(days=START_OFFSET_IN_DAYS)
            end_date = start_date + timedelta(days=NUMBER_OF_DAYS_TO_SCRAPE)
            while start_date < end_date:
                scrapers = markets[market]['scrapers']
                ret_offset = markets[market].get("ret", None)
                return_date = None
                if ret_offset:
                    return_date = start_date + timedelta(days=ret_offset)

                for scraper in scrapers:
                    rec = SingleScrapingJob()
                    rec.base_url = servers[random.randint(0, len(servers) - 1)]
                    rec.origin = market.split('-')[0]
                    rec.destination = market.split('-')[1]
                    rec.dept_date = start_date.strftime('%Y-%m-%d')
                    rec.return_date = return_date.strftime('%Y-%m-%d')
                    rec.scraper = scraper
                    jobs_queue.append(rec)
                start_date += timedelta(days=1)

    if strategy == "date_market":
        start_date = datetime.utcnow() + timedelta(days=START_OFFSET_IN_DAYS)
        end_date = start_date + timedelta(days=NUMBER_OF_DAYS_TO_SCRAPE)
        while start_date < end_date:
            for market in markets:
                scrapers = market['scrapers']
                ret_offset = market.get("ret", None)
                return_date = None
                if ret_offset:
                    return_date = start_date + timedelta(days=ret_offset)
                for scraper in scrapers:
                    rec = SingleScrapingJob()
                    rec.base_url = servers[random.randint(0, len(servers) - 1)]
                    rec.origin = market['origin']
                    rec.destination = market['destination']
                    rec.dept_date = start_date.strftime('%Y-%m-%d')
                    if return_date:
                        rec.return_date = return_date.strftime('%Y-%m-%d')
                    rec.scraper = scraper
                    logging.debug(f"Future scraping job:{rec}")
                    jobs_queue.append(rec)
            start_date += timedelta(days=1)

    return jobs_queue


def run_scraper_threaded(servers, markets, jobs_strategy="date_market"):
    logging.warning(f"started a new statistic")
    max_workers = (len(SERVERS) * JOBS_PER_SERVER) + 1
    logging.info(f"Scraping with max_workers={max_workers}")
    logging.info(f"Servers:{servers}")
    start_date = datetime.utcnow() + timedelta(days=START_OFFSET_IN_DAYS)
    end_date = start_date + timedelta(days=NUMBER_OF_DAYS_TO_SCRAPE)
    logging.info(f"Start date:{start_date}, end date:{end_date}")
    idx = 0
    job_queue = create_scrape_jobs(servers, markets, jobs_strategy)
    logging.info(f"Job queue size:{len(job_queue)}")
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        while idx < len(job_queue):
            job = job_queue[idx]
            executor.submit(scrape_job, job.base_url, job.origin, job.destination, job.dept_date, job.return_date,
                            job.scraper, idx, len(job_queue))
            idx += 1


run_scraper_threaded(SERVERS, MARKETS, "date_market")
