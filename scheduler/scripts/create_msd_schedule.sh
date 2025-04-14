#!/bin/bash
BASEDIR=$(dirname "$0")

#This script extracts flights (from scraped flights) and stores them in mongo 'msd_schedule' collection (used by MSD product matrix)
#we use only data scraped by ITASOFTWARE scraped as this is the one that correctly populates equipment codes
start_timestamp=`date '+%C%y%m%d%H%M%S' -d "-2 hour"`  #look at fares scraped 6hrs ago and after
start_departure=`date '+%C%y%m%d'`  #starting departure date for which schedule should be created
departure_days_in_future=200   #for how many days into the future we should populate schedule
source_scraper_name="itasoftware"   #antake only fares of this carrier
echo "Create MSD schedule, take fares scraped after $start_timestamp and scraped from $source_scraper_name, start dept: $start_departure "
source ${BASEDIR}/../.venv/bin/activate
cd ${BASEDIR}/../jobs
python3 create_msd_schedule.py $start_timestamp $start_departure $departure_days_in_future PBM AMS --source $source_scraper_name
python3 create_msd_schedule.py $start_timestamp $start_departure $departure_days_in_future AMS PBM --source $source_scraper_name
#python3 create_msd_schedule.py $start_timestamp $start_departure $departure_days_in_future YYJ YLW --source $source_scraper_name
#python3 create_msd_schedule.py $start_timestamp $start_departure $departure_days_in_future YLW YYJ --source $source_scraper_name

echo "Done"