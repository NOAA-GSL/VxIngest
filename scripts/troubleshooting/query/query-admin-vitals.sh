#!/usr/bin/env bash

# ***********************************************************************************************************
# query-admin-vitals.sh
#
#   Usage: ./query-admin-vitals.sh [options]
#
#   This will output the query admin vitals for one or more nodes in the cluster.
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
  vitals=$(curl \
    --user "$CB_USERNAME:$CB_PASSWORD" \
    --silent \
    --request GET \
    "$PROTOCOL://$node:$QUERY_PORT/admin/vitals")

  # output the vitals
  echo "$vitals" | \
    jq -r --arg node "$node" --arg query_port "$QUERY_PORT" '
    . as $vitals | $vitals |
    "****************************************************************" +
    "\n*  " + $node + ":" + ($query_port | tostring) + " Vitals" +
    "\n****************************************************************" +
    "\n  cores: " + ($vitals["cores"] | tostring) +
    "\n  cpu.user.percent: " + ($vitals["cpu.user.percent"] | tostring) +
    "\n  cpu.sys.percent: " + ($vitals["cpu.sys.percent"] | tostring) +
    "\n  gc.num: " + ($vitals["gc.num"] | tostring) +
    "\n  gc.pause.time: " + ($vitals["gc.pause.time"] | tostring) +
    "\n  gc.pause.percent: " + ($vitals["gc.pause.percent"] | tostring) +
    "\n  local.time: " + ($vitals["local.time"] | tostring) +
    "\n  memory.usage: " + ($vitals["memory.usage"] | tostring) +
    "\n  memory.total: " + ($vitals["memory.total"] | tostring) +
    "\n  memory.system: " + ($vitals["memory.system"] | tostring) +
    "\n  request.completed.count: " + ($vitals["request.completed.count"] | tostring) +
    "\n  request.active.count: " + ($vitals["request.active.count"] | tostring) +
    "\n  request.per.sec.1min: " + ($vitals["request.per.sec.1min"] | tostring) +
    "\n  request.per.sec.5min: " + ($vitals["request.per.sec.5min"] | tostring) +
    "\n  request.per.sec.15min: " + ($vitals["request.per.sec.15min"] | tostring) +
    "\n  request_time.mean: " + ($vitals["request_time.mean"] | tostring) +
    "\n  request_time.median: " + ($vitals["request_time.median"] | tostring) +
    "\n  request_time.80percentile: " + ($vitals["request_time.80percentile"] | tostring) +
    "\n  request_time.95percentile: " + ($vitals["request_time.95percentile"] | tostring) +
    "\n  request_time.99percentile: " + ($vitals["request_time.99percentile"] | tostring) +
    "\n  request.prepared.percent: " + ($vitals["request.prepared.percent"] | tostring) +
    "\n  total.threads: " + ($vitals["total.threads"] | tostring) +
    "\n  uptime: " + ($vitals["uptime"] | tostring) +
    "\n  version: " + ($vitals["version"] | tostring)
    '
done
