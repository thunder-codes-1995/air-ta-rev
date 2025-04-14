#!/bin/bash
BASEDIR=$(dirname "$0")
source ${BASEDIR}/../.venv/bin/activate
cd ${BASEDIR}/..
export PYTHONPATH=${BASEDIR}/..


#scrape_date=`date '+%C%y%m%d'`
file_date=`date '+%C%y%m%d'`
scrape_date=`date '+%C%y%m%d' -d "-6 hour"`
echo "Scrape date:$scrape_date, file_date:$file_date, create competitors fares and competitors CSV files"
python3 jobs/export_fares_csv.py  CY $scrape_date $file_date
echo "export done, uploading files to remote server"

HOST=34.91.202.215
PORT=2222
USERNAME=cyuser
PASSWORD=hu3Ck2L4zPAyP6yD
LOCAL_DATA_FOLDER=/tmp/output
COMPETITORS_REMOTE_FOLDER='/Atarev/Competitors'
COMPETITOR_FARES_REMOTE_FOLDER='/Atarev/Competitor Fares'
touch $LOCAL_DATA_FOLDER/competitor_fares_$file_date.tag
touch $LOCAL_DATA_FOLDER/competitors_$file_date.tag
sshpass -p $PASSWORD sftp -P $PORT $USERNAME@$HOST << EOF
put $LOCAL_DATA_FOLDER/competitor_fares_$file_date.csv '$COMPETITOR_FARES_REMOTE_FOLDER'
put $LOCAL_DATA_FOLDER/competitor_fares_$file_date.tag '$COMPETITOR_FARES_REMOTE_FOLDER'

put $LOCAL_DATA_FOLDER/competitors_$file_date.csv $COMPETITORS_REMOTE_FOLDER
put $LOCAL_DATA_FOLDER/competitors_$file_date.tag $COMPETITORS_REMOTE_FOLDER

exit
EOF
echo "All files were uploaded"
