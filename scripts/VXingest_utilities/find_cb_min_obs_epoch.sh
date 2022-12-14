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
# if it is a multinode host split on ',' and take the first one
IFS=','
read -ra hostarr <<< "$host"
host=${hostarr[0]}
user=`grep cb_user ${credentials_file} | awk '{print $2}'`
pwd=`grep cb_password ${credentials_file} | awk '{print $2}'`
bucket=$(grep cb_bucket ${credentials_file} | awk '{print $2}')
collection=$(grep cb_collection ${credentials_file} | awk '{print $2}')
scope=$(grep cb_scope ${credentials_file} | awk '{print $2}')
curl -s -u "${user}:${pwd}" http://${host}:8093/query/service  -d "statement=select min(METAR.fcstValidEpoch) as min_fcstValidEpoch, max(METAR.fcstValidEpoch) as max_fcstValidEpoch from ${bucket}.${scope}.${collection}  WHERE type='DD' and docType = 'obs' and subset = '${collection}' and version is not missing" | jq -r '.results | .[] | .min_fcstValidEpoch'
