#!/usr/bin/env bash

# ***********************************************************************************************************
# analytics-service-stats.sh
#
#   Usage: ./analytics-service-stats.sh [options]
#
#   This will output the analytics stats for just the service but not functions
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

curl \
  --user "$CB_USERNAME:$CB_PASSWORD" \
  --silent \
  "$PROTOCOL://$CLUSTER:$PORT/pools/default/buckets/@cbas/stats?zoom=$ZOOM" | \
  jq -r -c 'def roundit: .*100.0 + 0.5|floor/100.0 | .*100.0;
  .op.samples |
  "  cbas_disk_used: " + (try (.cbas_disk_used | add / length | roundit/100.0 | tostring) catch "N/A") +
  "\n  cbas_gc_count: " + (try (.cbas_gc_count | add / length | roundit/100.0 | tostring) catch "N/A") +
  "\n  cbas_gc_time: " + (try (.cbas_gc_time | add / length | roundit/100.0 | tostring) catch "N/A") +
  "\n  cbas_heap_used: " + (try (.cbas_heap_used | add / length | roundit/100.0 | tostring) catch "N/A") +
  "\n  cbas_io_reads: " + (try (.cbas_io_reads | add / length | roundit/100.0 | tostring) catch "N/A") +
  "\n  cbas_io_writes: " + (try (.cbas_io_writes | add / length | roundit/100.0 | tostring) catch "N/A") +
  "\n  cbas_system_load_average: " + (try (.cbas_system_load_average | add / length | roundit/100.0 | tostring) catch "N/A") +
  "\n  cbas_thread_count: " + (try (.cbas_thread_count | add / length | roundit/100.0 | tostring) catch "N/A")
  '
