#!/bin/sh
gitroot=$(git rev-parse --show-toplevel)
if [ "$gitroot" != "$(pwd)" ];then
        echo "$(pwd) is not a git root directory: cd to the clone root of VxIngest"
        exit
fi

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
cmd='update mdata set updated =  (select raw FLOOR(NOW_MILLIS()/1000))[0] where type="MD" and docType ="region" and version = "V01"  and subset="COMMON";'
cbimport json --cluster couchbase://${host} --bucket vxdata --scope-collection-exp _default.METAR --username ${user} --password ${pwd} --format list --generate-key %id% --dataset file://${PWD}/mats_metadata_and_indexes/metadata_files/regions.json
curl -s -u ${user}:${pwd} http://${host}:8093/query/service -d "statement=${cmd}"