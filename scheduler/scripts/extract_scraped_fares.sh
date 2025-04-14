#!/bin/bash
BASEDIR=$(dirname "$0")
#This script extracts scraped fares from raw_transformed collection and copies them to 'fares2' collection.
#It takes care of cabin name normalisation (economy, business, first)
#It also takes care of currency conversion
start_timestamp=`date '+%C%y%m%d%H%M%S' -d "-2 hour"`  #look at fares scraped 6hrs ago and after
echo "Extract scraped fares after $start_timestamp"
source ${BASEDIR}/../.venv/bin/activate
cd ${BASEDIR}/../jobs
python3 scraped_fares_etl_job.py $start_timestamp --source slm PY
python3 scraped_fares_etl_job.py $start_timestamp --source itasoftware --carrierexclude PY  PY
#python3 scraped_fares_etl_job.py $start_timestamp 8P
#python3 scraped_fares_etl_job.py $start_timestamp MNE --carrier MNE,WK,TK
echo "Done"