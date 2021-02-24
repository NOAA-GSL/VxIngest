#!/usr/bin/env bash

# ***********************************************************************************************************
# replication-statuses.sh
#
#   Usage: ./replication-statuses.sh [options]
#
#   This will output the xdcr replication statuses for the cluster.
#
#   Options:
#
#     --cluster=<s>           The cluster address (default: localhost)
#     --username=<s>          Cluster Admin or RBAC username (default: Administrator)
#     --password=<s>          Cluster Admin or RBAC password (default: password)
#     --port=<s>              The port to use (default: 8091)
#     --protocol=<s>          The protocol to use (default: http)
# ***********************************************************************************************************

# set the defaults, these can all be overriden as environment variables or passed via the cli
CB_USERNAME=${CB_USERNAME:='Administrator'}
CB_PASSWORD=${CB_PASSWORD:='password'}
CLUSTER=${CLUSTER:='localhost'}
PORT=${PORT:='8091'}
PROTOCOL=${PROTOCOL:='http'}

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

# get the remote cluster references
remoteClusters=$(curl \
  --user "$CB_USERNAME:$CB_PASSWORD" \
  --silent \
  "$PROTOCOL://$CLUSTER:$PORT/pools/default/remoteClusters" | \
  jq -r -c 'map( {
    (.uuid | tostring): {
      "name": .name,
      "hostname": .hostname
    }
  } ) | add')

# get the xdcr tasks
curl \
  --user "$CB_USERNAME:$CB_PASSWORD" \
  --silent \
  "$PROTOCOL://$CLUSTER:$PORT/pools/default/tasks" | \
  jq -r --argjson remote_clusters "$remoteClusters" '
  map(select(.type | contains("xdcr"))) | .[] |
  (.id | split("/")[0]) as $replicationId |
  (.id | split("/")[1]) as $sourceBucket |
  (.id | split("/")[2]) as $destBucket |
   . |
  "Replication: " + .id +
  "\n----------------------------------------------------------------" +
  "\nStatus: " + .status +
  ($remote_clusters | .[($replicationId | tostring)] |
    "\nRemote Cluster: " + .name +
    "\nHostname: " + .hostname
  ) +
  "\nSource Bucket: " + $sourceBucket +
  "\nDest Bucket: " + $destBucket +
  "\n"
  '