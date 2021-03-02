#!/bin/sh
if [ $# -ne 2 ]; then
  echo "Usage $0 backupdir server"
  exit 1
fi
if [[ ! -d "$1" ]]; then
  echo "$1 is not a directory - exiting"
  exit 1
fi
if [[ $1 == /* ]]; then
  backupdir=$1
else
  backupdir="./$1"
fi
server=$2

curl -v "http://$server:8093/query/service" -u gsd  -d 'statement=select meta().id, mdata.* from mdata where type="MD" and docType="ingest" and subset="METAR" and  version is not missing;' | jq '.results' > "$backupdir/ingest-$(date +%Y%m%d:%H%M%S)"
