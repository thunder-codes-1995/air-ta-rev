#!/bin/bash
BASEDIR=$(dirname "$0")

source ${BASEDIR}/../.venv/bin/activate
cd ${BASEDIR}/../events/conferences
python3 scraper.py
echo "Done"