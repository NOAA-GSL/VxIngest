#!/usr/bin/env bash

# ***********************************************************************************************************
# index-stats-dynamic-averaged.sh
#
#   Usage: ./index-stats-dynamic-averaged.sh [options]
#
#   This will output the index stats for all buckets in the cluster, and dynamically
#   output each of the available stats regardless of Couchbase Server version.  The
#   tradeoff to this approach is every field is averaged regardless, where sometimes
#   it makes more sense to sum the field
#
#   Options:
#
#     --cluster=<s>           The cluster address (default: localhost)
#     --username=<s>          Cluster Admin or RBAC username (default: Administrator)
#     --password=<s>          Cluster Admin or RBAC password (default: password)
#     --port=<s>              The port to use (default: 8091)
#     --protocol=<s>          The protocol to use (default: http)
#     --zoom=<s>              The option to sample minute, hour, day, week, year (default: minute)
# ***********************************************************************************************************

# set the defaults, these can all be overriden as environment variables or passed via the cli
CB_USERNAME=${CB_USERNAME:='Administrator'}
CB_PASSWORD=${CB_PASSWORD:='password'}
CLUSTER=${CLUSTER:='localhost'}
PORT=${PORT:='8091'}
PROTOCOL=${PROTOCOL:='http'}
ZOOM=${ZOOM:='minute'}

# parse any cli arguments
while [ $# -gt 0 ]; do
  case "$1" in
    --username=*)
      CB_USERNAME="${1#*=}"
      ;;
    --password=*)
      CB_PASSWORD="${1#*=}"
      ;;
    --cluster=*)
      CLUSTER="${1#*=}"
      ;;
    --port=*)
      PORT="${1#*=}"
      ;;
    --protocol=*)
      PROTOCOL="${1#*=}"
      ;;
    --zoom=*)
      ZOOM="${1#*=}"
      ;;
    *)
      printf "* Error: Invalid argument.*\n"
      exit 1
  esac
  shift
done

# make sure jq exists
if [ "$(command -v jq)" = "" ]; then
  echo >&2 "jq command is required, see (https://stedolan.github.io/jq/download)";
  exit 1;
fi

# loop over each of the buckets
for bucket in $(curl \
  --user "$CB_USERNAME:$CB_PASSWORD" \
  --silent \
  "$PROTOCOL://$CLUSTER:$PORT/pools/default/buckets" | \
  jq -r '.[] | .name')
do
  echo ""
  echo "Bucket: $bucket"
  echo "================================================================"
  # get the index stats for the bucket
  stats=$(curl \
    --user "$CB_USERNAME:$CB_PASSWORD" \
    --silent \
    "$PROTOCOL://$CLUSTER:$PORT/pools/default/buckets/@index-$bucket/stats?zoom=$ZOOM" | \
    jq -r -c '.op.samples | to_entries')

  # get the indexes for the bucket
  indexes=$(echo "$stats" | jq -r '.[] |
    select(.key | test("index\/[^\/]+\/.+")) |
    .key | split("/") | .[1]' | sort | uniq)

  # make sure the element has a length if it does output the stats
  if [[ ${indexes[0]} ]]; then
    # loop over each of the indexes
    # shellcheck disable=SC2068
    for index in ${indexes[@]}
    do
      echo "Index: $index"
      echo "----------------------------------------------------------------"
      echo "$stats" | jq -r -c "def roundit: .*100.0 + 0.5|floor/100.0 | .*100.0;
        . | sort_by(.key) | .[] |
        select(.key | test(\"index/$index.+\")) | \"  \" + (.key | split(\"/\") |
        .[-1:][0]) + \": \" + (.value | add / length | roundit/100.0 | tostring)"
      echo ""
    done
  else
    echo "No Indexes"
  fi
done
