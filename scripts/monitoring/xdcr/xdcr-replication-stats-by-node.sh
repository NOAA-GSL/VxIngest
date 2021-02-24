#!/usr/bin/env bash

# ***********************************************************************************************************
#  xdcr-replication-stats-by-node.sh
#
#   Usage: ./xdcr-replication-stats-by-node.sh [options]
#
#   This will output each of the xdcr replications, per bucket in the cluster
#
#   Options:
#
#     --cluster=<s>           The cluster address (default: localhost)
#     --username=<s>          Cluster Admin or RBAC username (default: Administrator)
#     --password=<s>          Cluster Admin or RBAC password (default: password)
#     --bucket=<s>            The bucket to use (default: default)
#     --port=<s>              The port to use (default: 8091)
#     --protocol=<s>          The protocol to use (default: http)
#     --zoom=<s>              The option to sample minute, hour, day, week, year (default: minute)
# ***********************************************************************************************************

# set the defaults, these can all be overriden as environment variables or passed via the cli
CB_USERNAME=${CB_USERNAME:='Administrator'}
CB_PASSWORD=${CB_PASSWORD:='password'}
CLUSTER=${CLUSTER:='localhost'}
BUCKET=${BUCKET:='default'}
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
    --bucket=*)
      BUCKET="${1#*=}"
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

# get all of the buckets in the cluster that have 1 or more
# xdcr replications configured
buckets=$(curl \
  --user "$CB_USERNAME:$CB_PASSWORD" \
  --silent \
  --request GET \
  "$PROTOCOL://$CLUSTER:$PORT/pools/default/tasks" | \
  jq -r '[ .[] | select(.type == "xdcr") | .source ] | sort | unique | .[]')

# get all of the nodes in the cluster running the data service
nodes=$(curl \
  --user "$CB_USERNAME:$CB_PASSWORD" \
  --silent \
  --request GET \
  "$PROTOCOL://$CLUSTER:$PORT/pools/default" | \
  jq -r '.nodes[] |
    select(.services | contains(["kv"]) == true) |
    .hostname'
)
# loop over each of the buckets
# shellcheck disable=SC2068
for bucket in ${buckets[@]}
do
  echo ""
  echo "Bucket: $bucket"
  echo "================================================================"
  # loop over each of the nodes in the cluster
  # shellcheck disable=SC2068
  for node in ${nodes[@]}
  do
    echo "Node: $node"
    echo "----------------------------------------------------------------"
    # get the xdcr stats for the bucket on the node
    curl \
      --user "$CB_USERNAME:$CB_PASSWORD" \
      --silent \
      --request GET \
      --data zoom=minute \
      "$PROTOCOL://$CLUSTER:$PORT/pools/default/buckets/@xdcr-$bucket/nodes/$node/stats?zoom=$ZOOM" | \
      jq -r '.op.samples | to_entries | sort_by(.key) | .[] |
        select(.key | split("/") | length > 1) |
        "  " + (.key) + ": " +
          (.value | add / length | tostring)'
    echo ""
  done
done
