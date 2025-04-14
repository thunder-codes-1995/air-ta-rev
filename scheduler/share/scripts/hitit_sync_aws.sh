source "$1.txt"
TODAY=$(date +"%Y%m%d")
BASEDIR=$(dirname "$0")
echo "$BASEDIR/$LOCAL_PATH"
# cp $BASEDIR/../.env.* $BASEDIR/../.env
aws s3 sync $S3 "$BASEDIR/$LOCAL_PATH" --include "*.$TODAY.*"
python3.8 $BASEDIR/../hitit_parse_csv.py
rm -fr "$BASEDIR/$LOCAL_PATH"
echo last updated at $(date +%Y%d%m_%H%M%S) > $BASEDIR/../hitit_sync_aws