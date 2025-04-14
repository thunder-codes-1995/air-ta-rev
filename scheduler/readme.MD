## Description

This repository contains various ETL scripts (ETL - Extract, Transform, Load) that are used by MSD and LFA for performing recurring tasks.
Below you will find descriptions of each script and it's parameters.
 

## Configuration

While being in a root project folder you need to:  
a) Create virtual environment  
`virtualenv .venv`

b) Activate it  
`source .venv/bin/activate`

c) Install required dependencies  
`pip install -r requirements.txt`

d) Create `.env` file with configuration.  
Use `.env_example` as a template

## Running

You must execute scripts from `jobs` folder  
`cd ./jobs`

Add current folder (jobs) and parent folder (root project folder) to PYTHONPATH env variable:  
`export PYTHONPATH=.:..`

Run script:
`python3 script_name.py`

## ETL Scripts

### scraped_fares_etl_job.py

This script does the following:

-   extracts 'RAW' scraped fares from 'scraped_fares_raw' collection
-   filters out unwanted fares (for example connecting flights, mixed carriers)
-   finds lowest available fare for each flight
-   converts currency based on conversion rates from DB
-   extract information about cabin&class
-   stores result in 'fares' collection

Using parameters you can add some filtering (e.g. export only fares for a given market or website)

```
usage: scraped_fares_etl_job.py [-h] [--departure DEPARTURE] [--origin ORIGIN] [--destination DESTINATION] [--type TYPE] [--source SOURCE] [--carrier CARRIER] [--dryrun DRYRUN] START_DATETIME

Extract raw scraped fares (matching criteria specified as arguments) and stores processed fares in destination collection

positional arguments:
  START_DATETIME : Start date/time(YYYYMMDDHHMMSS) from which this script should take fares from
  HOST : which config to use when uploading fares EX: PY
  ORIGIN : scrape only fares for flights flying from origin airport code
  DESTINATION : scrape only fares for flights flying to destination airport code
  SOURCE : filter only fares scraped from a specified website (e.g. flypgs.com, rome2rio.com)
  CARRIER : filter only fares for specified carrier code (e.g. TK, PC). Could be comma separated values

optional arguments:
  -h, --help            show this help message and exit
  --departure DEPARTURE
                        scrape only fares for flights departing on specifed date (YYYYMMDD)

  --dryrun DRYRUN       Process everything but do not save data into database
  --stay_duration
  --max_connections  Max legs count
```

### create_msd_schedule.py

Script creates MSD_SCHEDULE based on flights discovered during scraping

```
usage: create_msd_schedule.py [-h] [--source SOURCE] [--offset OFFSET] [--type TYPE] [--carrier CARRIER] [--dryrun DRYRUN] start_datetime departure days origin destination

Extract schedule from raw scraped fares (using criteria specified as arguments) and stores schedule in destination collection

positional arguments:
  start_datetime     Only records scraped after start_datetime(YYYYMMDDHHMMSS) will be taken into account
  departure          scrape only fares for flights departing on specifed date (YYYYMMDD)
  days               Number of days to scrape
  origin             scrape only fares for flights flying from origin airport code
  destination        scrape only fares for flights flying to destination airport code

optional arguments:
  -h, --help         show this help message and exit
  --source SOURCE    filter only fares scraped from a specified website (e.g. flypgs.com, rome2rio.com)
  --offset OFFSET    Start date(YYYYMMDD) offset from today
  --type TYPE        filter only specifed fare type (OW-One way, RT-Round trip (default)
  --carrier CARRIER  filter only fares for specified carrier code (e.g. TK, PC). Could be comma separated values
  --dryrun DRYRUN    Process everything but do not save data into database

```

### create_lfa_schedule_etl_job.py

Script creates LFA schedule based on flights discovered during scraping
LFA recommendations are processed for each flight for a host carrier.
Host carrier schedule needs to be created and this script does that.

```
usage: create_lfa_schedule_etl_job.py [-h] [--departure DEPARTURE] [--origin ORIGIN] [--destination DESTINATION] [--type TYPE] [--source SOURCE] [--carrier CARRIER] [--dryrun DRYRUN] START_DATETIME

Creates LFA Schedule from scraped fares data (matching criteria specified as arguments) and stores schedule in destination collection

positional arguments:
  START_DATETIME        Start date/time(YYYYMMDDHHMMSS) from which this script should take fares from

optional arguments:
  -h, --help            show this help message and exit
  --departure DEPARTURE
                        scrape only fares for flights departing on specifed date (YYYYMMDD)
  --origin ORIGIN       scrape only fares for flights flying from origin airport code
  --destination DESTINATION
                        scrape only fares for flights flying to destination airport code
  --type TYPE           filter only specifed fare type (OW-One way, RT-Round trip (default)
  --source SOURCE       filter only fares scraped from a specified website (e.g. flypgs.com, rome2rio.com)
  --carrier CARRIER     filter only fares for specified carrier code (e.g. TK, PC). Could be comma separated values
  --dryrun DRYRUN       Process everything but do not save data into database

```

### data_processing.py

Script is used to process incoming MSD data:

-   ticketing data (ticketing data from UPLIFT for flights departed in the past)
-   future data (sales data for flights departing in the future, from AMELIA system)

Script is performing preprocessing and eventually data loading into the DB

Processing depends on a value of `type` parameter

**historical_process**

-   load all UPLIFT data from input folder,
-   process it and store output in output folder in a file 'historical_data.csv'

**historical_store**

-   load already processed data from output folder,
-   store it in database (mysql + mongo)
-   once data is stored, delete forward looking data for departures for which we already have historical data

**forward_process**

-   load all AMELIA data from input folder,
-   process it and store output in output folder in a file 'forward_data.csv'
    **forward_store**
-   delete all forward looking data prior new batch is stored in the DB
-   load already processed forward looking data from 'forward_data.csv'
-   store it in the database (mysql + mongo)

```
usage: data_processing.py [-h] [--dryrun DRYRUN] [--tempfiles TEMPFILES] type snapshot_date

Process historical(uplift) or forward looking(amelia) data, store it in the database

positional arguments:
  type                  historical_process|historical_store|forward_process|forward_store
  snapshot_date         date of data snapshot (YYYY-MM-DD)

optional arguments:
  -h, --help            show this help message and exit
  --dryrun DRYRUN       Process everything but do not save data into database
  --tempfiles TEMPFILES
                        Store intermediate CSV files in temporary folder
```
