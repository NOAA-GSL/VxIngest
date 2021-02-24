#!/bin/sh
if [ $# -ne 1 ]; then
  echo "Usage $0 backupdir"
  exit 1
fi
if [[ $1 == /* ]]; then
  backupdir=$1
else
  backupdir="./$1"
fi
echo cbtransfer http://localhost:8091 csv://$backupdir/ingest.csv -u gsd -p 'gsd_pwd_av!d' -b mdata --silent -k 'MD.*:ingest'
cbtransfer http://localhost:8091 csv:///$backupdir/ingest.csv -u gsd -p 'gsd_pwd_av!d' -b mdata --silent -k 'MD.*:ingest'