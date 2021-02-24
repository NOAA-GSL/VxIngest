#!/usr/bin/env bash

# set the cluster username / password
CB_USERNAME='Administrator'
CB_PASSWORD='password'
BUCKET='travel-sample'

# make sure jq exists
if [ "$(command -v jq)" = "" ]; then
  echo >&2 "jq command is required, see (https://stedolan.github.io/jq/download)";
  exit 1;
fi

# get the fts stats for the bucket
curl \
  --user "$CB_USERNAME:$CB_PASSWORD" \
  --silent \
  http://localhost:8091/pools/default/buckets/@fts-$BUCKET/stats | \
  jq -r -c 'def roundit: .*100.0 + 0.5|floor/100.0 | .*100.0;
  .op.samples | to_entries | sort_by(.key) | .[] |
  select(.key != "timestamp" and (.key | split("/") | length == 2)) |
  (.key | split("/") | .[1]) + ": " + (.value | add / length | roundit/100.0 | tostring)'
