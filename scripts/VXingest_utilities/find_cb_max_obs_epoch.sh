#!/bin/sh
if [ $# -ne 1 ]; then
  echo "Usage $0 credentials-file"
  exit 1
fi
if [[ ! -f "$1" ]]; then
  echo "$1 is not a file - exiting"
  exit 1
fi
credentials_file=$1
host=`grep cb_host ${credentials_file} | awk '{print $2}'`
user=`grep cb_user ${credentials_file} | awk '{print $2}'`
pwd=`grep cb_password ${credentials_file} | awk '{print $2}'`

curl -s -u "${user}:${pwd}" http://${host}:8093/query/service  -d 'statement=select min(mdata.fcstValidEpoch) as minfcstValidEpoch, max(mdata.fcstValidEpoch) as max_fcstValidEpoch from mdata WHERE type="DD" and docType = "obs" and subset = "METAR" and version is not missing' | jq -r '.results | .[] | .max_fcstValidEpoch'
