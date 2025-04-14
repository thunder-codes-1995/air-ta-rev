#!/bin/bash
BASEDIR=$(dirname "$0")

#This script runs scraping python script
echo "Start scraping"
source ${BASEDIR}/../.venv/bin/activate
cd ${BASEDIR}/../jobs
python3 scrapers_test_script.py
echo "Done scraping"