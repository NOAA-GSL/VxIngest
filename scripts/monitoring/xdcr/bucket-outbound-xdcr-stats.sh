#!/usr/bin/env bash

# ***********************************************************************************************************
# bucket-incoming-xdcr-stats.sh
#
#   Usage: ./replication-statuses.sh [options]
#
#   This will output the incoming xdcr stats for the bucket
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

curl \
  --user "$CB_USERNAME:$CB_PASSWORD" \
  --silent \
  "$PROTOCOL://$CLUSTER:$PORT/pools/default/buckets/$BUCKET/stats?zoom=$ZOOM" | \
  jq -r '.op.samples |
  "ep_dcp_xdcr_backoff: " + (try (.ep_dcp_xdcr_backoff | add / length | tostring ) catch "N/A") +
  "\nep_dcp_xdcr_count: " + (try (.ep_dcp_xdcr_count | add / length | tostring ) catch "N/A") +
  "\nep_dcp_xdcr_items_remaining: " + (try (.ep_dcp_xdcr_items_remaining | add / length | tostring ) catch "N/A") +
  "\nep_dcp_xdcr_items_sent: " + (try (.ep_dcp_xdcr_items_sent | add / length | tostring ) catch "N/A") +
  "\nep_dcp_xdcr_producer_count: " + (try (.ep_dcp_xdcr_producer_count | add / length | tostring ) catch "N/A") +
  "\nep_dcp_xdcr_total_backlog_size: " + (try (.ep_dcp_xdcr_total_backlog_size | add / length | tostring ) catch "N/A") +
  "\nep_dcp_xdcr_total_bytes: " + (try (.ep_dcp_xdcr_total_bytes | add / length | tostring ) catch "N/A")
  '
