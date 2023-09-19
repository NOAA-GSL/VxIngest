#!/bin/sh
if [ $# -ne 2 ]; then
  echo "Usage $0 credentials-file schedule"
  exit 1
fi
if [[ ! -f "$1" ]]; then
  echo "$1 is not a file - exiting"
  exit 1
fi
num_fields=$(echo "$2" | awk '{print NF}')
if [[ $num_fields -ne 5 ]]; then
  echo "$2 does not have 5 fileds e.g. '0 * * * *' - exiting"
  exit 1
fi
credentials_file=$1
schedule=$2
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

curl -s -u "${user}:${pwd}" http://${host}:8093/query/service  -d "statement=UPDATE ${bucket}.${scope}.${collection} SET schedule = \"${schedule}\" WHERE type=\"JOB\" AND version = \"V01\" AND subset = \"${collection}\"" 
