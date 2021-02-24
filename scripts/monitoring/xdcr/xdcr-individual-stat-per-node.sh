#!/usr/bin/env bash

# set the cluster username / password
CB_USERNAME='Administrator'
CB_PASSWORD='password'

# set the replication info
REMOTE_CLUSTER='20763b82bb6b517bd0d15d9f6b78c13c'
SOURCE_BUCKET='travel-sample'
DESTINATION_BUCKET='demo'
STAT_NAME='percent_completeness'

# build the url
STAT_URL="http://localhost:8091/pools/default/buckets/$SOURCE_BUCKET/stats"
STAT_URL="$STAT_URL/replications%2F$REMOTE_CLUSTER%2F$SOURCE_BUCKET"
STAT_URL="$STAT_URL%2F$DESTINATION_BUCKET%2F$STAT_NAME"

# make sure jq exists
if [ "$(command -v jq)" = "" ]; then
  echo >&2 "jq command is required, see (https://stedolan.github.io/jq/download)";
  exit 1;
fi

curl \
  --user "$CB_USERNAME:$CB_PASSWORD" \
  --silent \
  $STAT_URL | \
  jq -r '.nodeStats | to_entries | .[] | (.key | split(":") | .[0]) + ": " + (.value | add / length | tostring)'