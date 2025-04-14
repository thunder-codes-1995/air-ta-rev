#!/bin/bash
BASEDIR=$(dirname "$0")

source ${BASEDIR}/../.venv/bin/activate
cd ${BASEDIR}/../handlers/sv/load_factor_sftp
python3 run.py
echo "Done"