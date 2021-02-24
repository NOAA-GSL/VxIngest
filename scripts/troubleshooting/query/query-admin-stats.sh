#!/usr/bin/env bash

# ***********************************************************************************************************
# query-admin-stats.sh
#
#   Usage: ./query-admin-stats.sh [options]
#
#   This will output the query admin stats for one or more nodes in the cluster.
#
#   Options:
#
#     --cluster=<s>           The cluster address (default: localhost)
#     --username=<s>          Cluster Admin or RBAC username (default: Administrator)
#     --password=<s>          Cluster Admin or RBAC password (default: password)
#     --port=<s>              The port to use (default: 8091)
#     --protocol=<s>          The protocol to use (default: http)
#     --nodes=<s>             A comma-delimited list of index node hostnames/ips, all nodes are included by default (default: "*")
#     --queryPort=<s>         The query port to use (default: 8093)
# ***********************************************************************************************************

# set the defaults, these can all be overriden as environment variables or passed via the cli
CB_USERNAME=${CB_USERNAME:='Administrator'}
CB_PASSWORD=${CB_PASSWORD:='password'}
CLUSTER=${CLUSTER:='localhost'}
PORT=${PORT:='8091'}
PROTOCOL=${PROTOCOL:='http'}
NODES=${NODES:='*'}
QUERY_PORT=${QUERY_PORT:='8093'}

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
    --nodes=*)
      NODES="${1#*=}"
      ;;
    --queryPort=*)
      QUERY_PORT="${1#*=}"
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
# if nodes is all, then get a list of query nodes from the cluster manager
if [[ "$NODES" == "*" ]]; then
  # get the query nodes
  NODES=$(curl \
    --user "$CB_USERNAME:$CB_PASSWORD" \
    --silent \
    --request GET \
    "$PROTOCOL://$CLUSTER:$PORT/pools/nodes" | \
    jq -r '.nodes[] |
      select(.services | contains(["n1ql"]) == true) |
      # get just the hostname without the port
      .hostname | split(":")[0]'
    )
else
  # convert the passed list into an array
  IFS=', ' read -r -a NODES <<< "$NODES"
fi

# loop over each of the index nodes
# shellcheck disable=SC2068
for node in ${NODES[@]}
do
  # get the index admin stats
  stats=$(curl \
    --user "$CB_USERNAME:$CB_PASSWORD" \
    --silent \
    --request GET \
    "$PROTOCOL://$node:$QUERY_PORT/admin/stats")

  # output the service level-stats
  echo "$stats" | \
    jq -r --arg node "$node" --arg query_port "$QUERY_PORT" '
    . as $stats | $stats |
    "****************************************************************" +
    "\n*  " + $node + ":" + ($query_port | tostring) + " Stats" +
    "\n****************************************************************" +
    "\n  active_requests.count: " + ($stats["active_requests.count"] | tostring) +
    "\n  at_plus.count: " + ($stats["at_plus.count"] | tostring) +
    "\n  cancelled.count: " + ($stats["cancelled.count"] | tostring) +
    "\n  deletes.count: " + ($stats["deletes.count"] | tostring) +
    "\n  errors.count: " + ($stats["errors.count"] | tostring) +
    "\n  inserts.count: " + ($stats["inserts.count"] | tostring) +
    "\n  invalid_requests.count: " + ($stats["invalid_requests.count"] | tostring) +
    "\n  mutations.count: " + ($stats["mutations.count"] | tostring) +
    "\n  prepared.15m.rate: " + ($stats["prepared.15m.rate"] | tostring) +
    "\n  prepared.1m.rate: " + ($stats["prepared.1m.rate"] | tostring) +
    "\n  prepared.5m.rate: " + ($stats["prepared.5m.rate"] | tostring) +
    "\n  prepared.count: " + ($stats["prepared.count"] | tostring) +
    "\n  prepared.mean.rate: " + ($stats["prepared.mean.rate"] | tostring) +
    "\n  queued_requests.count: " + ($stats["queued_requests.count"] | tostring) +
    "\n  request_rate.15m.rate: " + ($stats["request_rate.15m.rate"] | tostring) +
    "\n  request_rate.1m.rate: " + ($stats["request_rate.1m.rate"] | tostring) +
    "\n  request_rate.5m.rate: " + ($stats["request_rate.5m.rate"] | tostring) +
    "\n  request_rate.count: " + ($stats["request_rate.count"] | tostring) +
    "\n  request_rate.mean.rate: " + ($stats["request_rate.mean.rate"] | tostring) +
    "\n  request_time.count: " + ($stats["request_time.count"] | tostring) +
    "\n  request_timer.15m.rate: " + ($stats["request_timer.15m.rate"] | tostring) +
    "\n  request_timer.1m.rate: " + ($stats["request_timer.1m.rate"] | tostring) +
    "\n  request_timer.5m.rate: " + ($stats["request_timer.5m.rate"] | tostring) +
    "\n  request_timer.75%: " + ($stats["request_timer.75%"] | tostring) +
    "\n  request_timer.95%: " + ($stats["request_timer.95%"] | tostring) +
    "\n  request_timer.99%: " + ($stats["request_timer.99%"] | tostring) +
    "\n  request_timer.99.9%: " + ($stats["request_timer.99.9%"] | tostring) +
    "\n  request_timer.count: " + ($stats["request_timer.count"] | tostring) +
    "\n  request_timer.max: " + ($stats["request_timer.max"] | tostring) +
    "\n  request_timer.mean: " + ($stats["request_timer.mean"] | tostring) +
    "\n  request_timer.mean.rate: " + ($stats["request_timer.mean.rate"] | tostring) +
    "\n  request_timer.median: " + ($stats["request_timer.median"] | tostring) +
    "\n  request_timer.min: " + ($stats["request_timer.min"] | tostring) +
    "\n  request_timer.stddev: " + ($stats["request_timer.stddev"] | tostring) +
    "\n  requests.count: " + ($stats["requests.count"] | tostring) +
    "\n  requests_1000ms.count: " + ($stats["requests_1000ms.count"] | tostring) +
    "\n  requests_250ms.count: " + ($stats["requests_250ms.count"] | tostring) +
    "\n  requests_5000ms.count: " + ($stats["requests_5000ms.count"] | tostring) +
    "\n  requests_500ms.count: " + ($stats["requests_500ms.count"] | tostring) +
    "\n  result_count.count: " + ($stats["result_count.count"] | tostring) +
    "\n  result_size.count: " + ($stats["result_size.count"] | tostring) +
    "\n  scan_plus.count: " + ($stats["scan_plus.count"] | tostring) +
    "\n  selects.count: " + ($stats["selects.count"] | tostring) +
    "\n  service_time.count: " + ($stats["service_time.count"] | tostring) +
    "\n  unbounded.count: " + ($stats["unbounded.count"] | tostring) +
    "\n  updates.count: " + ($stats["updates.count"] | tostring) +
    "\n  warnings.count: " + ($stats["warnings.count"] | tostring)
    '
done
