#!/usr/bin/env bash

# ***********************************************************************************************************
# index-service-stats-for-node.sh
#
#   Usage: ./index-service-stats-for-node.sh [options]
#
#   This will output the index service ram usage in the cluster
#
#   Options:
#
#     --cluster=<s>           The cluster address (default: localhost)
#     --username=<s>          Cluster Admin or RBAC username (default: Administrator)
#     --password=<s>          Cluster Admin or RBAC password (default: password)
#     --port=<s>              The port to use (default: 8091)
#     --node=<s>              A node hostname / ip in the cluster (default: localhost:8091)
#     --protocol=<s>          The protocol to use (default: http)
#     --zoom=<s>              The option to sample minute, hour, day, week, year (default: minute)
# ***********************************************************************************************************

# set the defaults, these can all be overriden as environment variables or passed via the cli
CB_USERNAME=${CB_USERNAME:='Administrator'}
CB_PASSWORD=${CB_PASSWORD:='password'}
CLUSTER=${CLUSTER:='localhost'}
PORT=${PORT:='8091'}
NODE=${NODE:='localhost:8091'}
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
    --node=*)
      NODE="${1#*=}"
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

curl \
  --user "$CB_USERNAME:$CB_PASSWORD" \
  --silent \
  "$PROTOCOL://$CLUSTER:$PORT/pools/default/buckets/@index/nodes/$NODE/stats?zoom=$ZOOM" | \
  jq -r 'def roundit: .*100.0 + 0.5|floor/100.0 | .*100.0;
  .op.samples |
  "index_memory_quota: " + (try (.index_memory_quota | add / length | roundit/100.0 | tostring) catch "N/A") +
  "\nindex_memory_used: " + (try (.index_memory_used | add / length | roundit/100.0 | tostring) catch "N/A") +
  "\nindex_ram_percent: " + (try (.index_ram_percent | add / length | roundit/100.0 | tostring) catch "N/A") +
  "\nindex_remaining_ram: " + (try (.index_remaining_ram | add / length | roundit/100.0 | tostring) catch "N/A")
  '
