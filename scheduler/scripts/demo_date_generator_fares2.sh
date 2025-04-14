!/bin/bash
if [ "$#" -ne 5 ]; then
    echo "Usage: $0 carrier_code origin destination year month"
    exit 1
fi
BASEDIR=$(dirname "$0")

source ${BASEDIR}/../.venv/bin/activate
cd ${BASEDIR}/../demo_data_generator
python3 fares2.py "$1" "$2" "$3" "$4" "$5"
echo "Done"