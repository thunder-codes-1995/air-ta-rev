TODAY=$(date +"%Y%m%d")
BASEDIR=$(dirname "$0")
source "$BASEDIR/../$1.txt"
#source ${BASEDIR}/../env/bin/activate
# cp $BASEDIR/../.env.* $BASEDIR/../.env
python3.8 $BASEDIR/../hitit_create_authorization.py
#rsync -avz $BASEDIR/../authorization/hitit ubuntu@54.74.74.177:/opt/hitit/scripts/authorization
rsync -avz -e "ssh -i /home/ec2-user/.ssh/ftpserver.pem" $BASEDIR/../authorization/hitit ubuntu@54.74.74.177:/opt/hitit/outgoing
rm -fr authorization
echo last updated at $(date +%Y%d%m_%H%M%S) > $BASEDIR/../hitit_create_authroization