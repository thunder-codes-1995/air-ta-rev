#!/bin/bash
BASEDIR=$(dirname "$0")
source ${BASEDIR}/../.venv/bin/activate
cd ${BASEDIR}/..
export PYTHONPATH=${BASEDIR}/..
python3 lfa_queue_jobs/queue_consumer_create_lfa_schedule.py
echo "Done"