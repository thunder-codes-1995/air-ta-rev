#!/bin/bash
BASEDIR=$(dirname "$0")
source ${BASEDIR}/../.venv/bin/activate
cd ${BASEDIR}/..
export PYTHONPATH=${BASEDIR}/..
# valid percentage: at least 10%
python3 servers_monitor/monitor.py 80
echo "The servers has been checked"
