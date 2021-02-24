#!/usr/bin/env bash

# ***********************************************************************************************************
# system-stats-individual-node.sh
#
#   Usage: ./system-stats-for-node.sh [options]
#
#   This will output the system stats for a specific node
#
#   Options:
#
#     --cluster=<s>           The cluster address (default: localhost)
#     --username=<s>          Cluster Admin or RBAC username (default: Administrator)
#     --password=<s>          Cluster Admin or RBAC password (default: password)
#     --node=<s>              A node hostname / ip in the cluster (default: localhost:8091)
#     --port=<s>              The port to use (default: 8091)
#     --protocol=<s>          The protocol to use (default: http)
#     --zoom=<s>              The option to sample minute, hour, day, week, year (default: minute)
# ***********************************************************************************************************

# set the defaults, these can all be overriden as environment variables or passed via the cli
CB_USERNAME=${CB_USERNAME:='Administrator'}
CB_PASSWORD=${CB_PASSWORD:='password'}
CLUSTER=${CLUSTER:='localhost'}
NODE=${NODE:='localhost:8091'}
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
  "$PROTOCOL://$CLUSTER:$PORT/pools/default/buckets/@system/nodes/$NODE/stats?zoom=$ZOOM" | \
  jq -r -c "def roundit: .*100.0 + 0.5|floor/100.0 | .*100.0;
  .op.samples |
  \"  cpu_idle_ms: \" + (try (.cpu_idle_ms | (. | add / length | roundit/100.0 | tostring)) catch \"N/A\") +
  \"\n  checkpoint_failure_count: \" + (try (.cpu_local_ms | (. | add / length | roundit/100.0 | tostring)) catch \"N/A\") +
  \"\n  cpu_utilization_rate: \" + (try (.cpu_utilization_rate | (. | add / length | roundit/100.0 | tostring)) catch \"N/A\") +
  \"\n  hibernated_requests: \" + (try (.hibernated_requests | (. | add / length | roundit/100.0 | tostring)) catch \"N/A\") +
  \"\n  hibernated_waked: \" + (try (.hibernated_waked | (. | add / length | roundit/100.0 | tostring)) catch \"N/A\") +
  \"\n  mem_actual_free: \" + (try (.mem_actual_free | (. | add / length | roundit/100.0 | tostring)) catch \"N/A\") +
  \"\n  mem_actual_used: \" + (try (.mem_actual_used | (. | add / length | roundit/100.0 | tostring)) catch \"N/A\") +
  \"\n  mem_free: \" + (try (.mem_free | (. | add / length | roundit/100.0 | tostring)) catch \"N/A\") +
  \"\n  mem_total: \" + (try (.mem_total | (. | add / length | roundit/100.0 | tostring)) catch \"N/A\") +
  \"\n  mem_used_sys: \" + (try (.mem_used_sys | (. | add / length | roundit/100.0 | tostring)) catch \"N/A\") +
  \"\n  rest_requests: \" + (try (.rest_requests | (. | add / length | roundit/100.0 | tostring)) catch \"N/A\") +
  \"\n  swap_total: \" + (try (.swap_total | (. | add / length | roundit/100.0 | tostring)) catch \"N/A\") +
  \"\n  swap_used: \" + (try (.swap_used | (. | add / length | roundit/100.0 | tostring)) catch \"N/A\")
  "
