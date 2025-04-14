#!/bin/bash
BASEDIR=$(dirname "$0")

source ${BASEDIR}/../.venv/bin/activate
cd ${BASEDIR}/../events/schools
python3 scraper.py
echo "Done"