TODAY=$(date +"%Y%m%d")
BASEDIR=$(dirname "$0")
PEM_FILE_PATH=/opt/arev/.ssh
#PEM_FILE_PATH=~/.ssh
PATH_TO_LF_FILE=${BASEDIR}/..

echo pulling file lf.${TODAY}.csv.............
sftp -i $PEM_FILE_PATH/atarev-dev-sftp-cyprus.pem atarev-dev-sftp-cyprus@sftp.atarev.com << EOF
get lf.${TODAY}.csv ${PATH_TO_LF_FILE}
exit
EOF

source ${BASEDIR}/../../.venv/bin/activate
cd ${BASEDIR}/../..
pwd
echo processing file lf.${TODAY}.csv.............
python share/cyprus_load_factor.py $TODAY $PATH_TO_LF_FILE
echo deleting  file lf.${TODAY}.csv.............

echo "Done !"


