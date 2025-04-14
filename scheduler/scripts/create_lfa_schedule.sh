#!/bin/bash
BASEDIR=$(dirname "$0")
#This script extracts flights (from scraped host flights) and stores them in mongo 'schedule' collection (used by LFA)
start_timestamp=`date '+%C%y%m%d%H%M%S' -d "-2 hour"`  #look at fares scraped 6hrs ago and after
carrier_code="PY"   #take only fares of this carrier
echo "Create LFA schedule, take fares scraped after $start_timestamp and for carrier: $carrier_code"
source ${BASEDIR}/../.venv/bin/activate
cd ${BASEDIR}/../jobs
python3 create_lfa_schedule_etl_job.py $start_timestamp --carrier $carrier_code
#python3 create_lfa_schedule_etl_job.py $start_timestamp --carrier 8P
echo "Done"