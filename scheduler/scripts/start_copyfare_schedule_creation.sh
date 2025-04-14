#!/bin/bash
BASEDIR=$(dirname "$0")
source ${BASEDIR}/../.venv/bin/activate
cd ${BASEDIR}/..
export PYTHONPATH=${BASEDIR}/..
python3 lfa_queue_jobs/schedule_processor.py $1
echo "Done"