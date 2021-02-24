#!/usr/bin/env bash

# ***********************************************************************************************************
# eventing-functions-stats-by-node.sh
#
#   Usage: ./eventing-functions-stats-individual-node.sh [options]
#
#   This will output the eventing stats for all functions for each node in the cluster
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
for node in $(curl \
  --user "$CB_USERNAME:$CB_PASSWORD" \
  --silent \
  "$PROTOCOL://$CLUSTER:$PORT/pools/default" | \
  jq -r '.nodes[] |
    select(.services | contains(["eventing"]) == true) |
    .hostname'
  )
do
  echo "$node Function Stats"
  echo "-------------------------------------------------------"
  # get the eventing stats
  curl \
    --user "$CB_USERNAME:$CB_PASSWORD" \
    --silent \
    "$PROTOCOL://$CLUSTER:$PORT/pools/default/buckets/@eventing/nodes/$node/stats?zoom=$ZOOM" | jq -r 'def roundit: .*100.0 + 0.5|floor/100.0 | .*100.0;
    .op.samples | map_values(. | add / length | roundit/100.0) as $stats
    | [
      keys | .[] | select(. | split("/") | length == 3) | split("/")[1]
    ] | sort | unique as $funcs
    | $funcs | .[] |
    "Function: " + . +
    "\n  bucket_op_exception_count: " + (try ($stats["eventing/" + . + "/bucket_op_exception_count"] | tostring) catch "N/A") +
    "\n  checkpoint_failure_count: " + (try ($stats["eventing/" + . + "/checkpoint_failure_count"] | tostring) catch "N/A") +
    "\n  dcp_backlog: " + (try ($stats["eventing/" + . + "/dcp_backlog"] | tostring) catch "N/A") +
    "\n  failed_count: " + (try ($stats["eventing/" + . + "/failed_count"] | tostring) catch "N/A") +
    "\n  n1ql_op_exception_count: " + (try ($stats["eventing/" + . + "/n1ql_op_exception_count"] | tostring) catch "N/A") +
    "\n  on_delete_failure: " + (try ($stats["eventing/" + . + "/on_delete_failure"] | tostring) catch "N/A") +
    "\n  on_delete_success: " + (try ($stats["eventing/" + . + "/on_delete_success"] | tostring) catch "N/A") +
    "\n  on_update_failure: " + (try ($stats["eventing/" + . + "/on_update_failure"] | tostring) catch "N/A") +
    "\n  on_update_success: " + (try ($stats["eventing/" + . + "/on_update_success"] | tostring) catch "N/A") +
    "\n  processed_count: " + (try ($stats["eventing/" + . + "/processed_count"] | tostring) catch "N/A") +
    "\n  timeout_count: " + (try ($stats["eventing/" + . + "/timeout_count"] | tostring) catch "N/A")
  '
done
