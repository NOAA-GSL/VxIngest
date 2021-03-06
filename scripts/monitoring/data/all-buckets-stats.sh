#!/usr/bin/env bash

# set the cluster username / password
CB_USERNAME='Administrator'
CB_PASSWORD='password'

# make sure jq exists
if [ "$(command -v jq)" = "" ]; then
  echo >&2 "jq command is required, see (https://stedolan.github.io/jq/download)";
  exit 1;
fi

# loop over each of the buckets
for bucket in $(curl \
  --user "$CB_USERNAME:$CB_PASSWORD" \
  --silent \
  http://localhost:8091/pools/default/buckets | \
  jq -r '.[] | .name')
do
  echo ""
  echo "Bucket: $bucket"
  echo "================================================================"
  # output the stats for the bucket
  curl \
    --user "$CB_USERNAME:$CB_PASSWORD" \
    --silent \
    "http://localhost:8091/pools/default/buckets/$bucket/stats" | \
    jq -r -c 'def roundit: .*100.0 + 0.5|floor/100.0 | .*100.0;
    .op.samples | to_entries | sort_by(.key) | .[] |
    "  " + (.key) + ": " + (.value | add / length | roundit/100.0 | tostring)'
done
