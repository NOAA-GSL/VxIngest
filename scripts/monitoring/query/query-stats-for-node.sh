#!/usr/bin/env bash

# ***********************************************************************************************************
# query-stats-for-node.sh
#
#   Usage: ./query-stats-for.sh [options]
#
#   This will output the query stats for the service on a specific node
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
  "$PROTOCOL://$CLUSTER:$PORT/pools/default/buckets/@query/nodes/$NODE/stats?zoom=$ZOOM" | \
  jq -r "def roundit: .*100.0 + 0.5|floor/100.0 | .*100.0;
  .op.samples |
  \"  query_avg_req_time: \" + (try (.query_avg_req_time | (. | add / length | roundit/100.0 | tostring)) catch \"N/A\") +
  \"\n  query_avg_svc_time: \" + (try (.query_avg_svc_time | (. | add / length | roundit/100.0 | tostring)) catch \"N/A\") +
  \"\n  query_avg_response_size: \" + (try (.query_avg_response_size | (. | add / length | roundit/100.0 | tostring)) catch \"N/A\") +
  \"\n  query_avg_result_count: \" + (try (.query_avg_result_count | (. | add / length | roundit/100.0 | tostring)) catch \"N/A\") +
  \"\n  query_active_requests: \" + (try (.query_active_requests | (. | add / length | roundit/100.0 | tostring)) catch \"N/A\") +
  \"\n  query_errors: \" + (try (.query_errors | (. | add / length | roundit/100.0 | tostring)) catch \"N/A\") +
  \"\n  query_invalid_requests: \" + (try (.query_invalid_requests | (. | add / length | roundit/100.0 | tostring)) catch \"N/A\") +
  \"\n  query_queued_requests: \" + (try (.query_queued_requests | (. | add / length | roundit/100.0 | tostring)) catch \"N/A\") +
  \"\n  query_request_time: \" + (try (.query_request_time | (. | add / length | roundit/100.0 | tostring)) catch \"N/A\") +
  \"\n  query_requests: \" + (try (.query_requests | (. | add / length | roundit/100.0 | tostring)) catch \"N/A\") +
  \"\n  query_requests_1000ms: \" + (try (.query_requests_1000ms | (. | add / length | roundit/100.0 | tostring)) catch \"N/A\") +
  \"\n  query_requests_250ms: \" + (try (.query_requests_250ms | (. | add / length | roundit/100.0 | tostring)) catch \"N/A\") +
  \"\n  query_requests_5000ms: \" + (try (.query_requests_5000ms | (. | add / length | roundit/100.0 | tostring)) catch \"N/A\") +
  \"\n  query_requests_500ms: \" + (try (.query_requests_500ms | (. | add / length | roundit/100.0 | tostring)) catch \"N/A\") +
  \"\n  query_result_count: \" + (try (.query_result_count | (. | add / length | roundit/100.0 | tostring)) catch \"N/A\") +
  \"\n  query_result_size: \" + (try (.query_result_size | (. | add / length | roundit/100.0 | tostring)) catch \"N/A\") +
  \"\n  query_selects: \" + (try (.query_selects | (. | add / length | roundit/100.0 | tostring)) catch \"N/A\") +
  \"\n  query_service_time: \" + (try (.query_service_time | (. | add / length | roundit/100.0 | tostring)) catch \"N/A\") +
  \"\n  query_warnings: \" + (try (.query_warnings | (. | add / length | roundit/100.0 | tostring)) catch \"N/A\")
  "
