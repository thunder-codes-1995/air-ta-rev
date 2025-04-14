TODAY=$(date +"%Y%m%d")
BASEDIR=$(dirname "$0")
source "$BASEDIR/../$1.txt"
#source ${BASEDIR}/../env/bin/activate
# cp $BASEDIR/../.env.* $BASEDIR/../.env
python3.8 $BASEDIR/../hitit_update_inventory.py $(date +"%Y-%m-%d") $2
echo last updated at $(date +%Y%d%m_%H%M%S) > $BASEDIR/../hitit_update_inventory