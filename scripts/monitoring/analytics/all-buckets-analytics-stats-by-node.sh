#!/usr/bin/env bash

# ***********************************************************************************************************
# all-buckets-analytics-stats-by-node.sh
#
#   Usage: ./all-buckets-analytics-stats-by-node.sh [options]
#
#   This will output the analytics stats for each node and bucket in the cluster, and explicitly
#   reference each of the available stats, and will correctly sum() or average()
#   the appropriate properties
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

# get all of the buckets using analytics
buckets=$(curl \
  --user "$CB_USERNAME:$CB_PASSWORD" \
  --silent \
  --data-urlencode "statement=SELECT Name FROM \`Metadata\`.\`Bucket\`;" \
  "$PROTOCOL://$CLUSTER:8095/analytics/service" | \
  jq -r '.results[].Name')

# loop over each of the nodes in the cluster
for node in $(curl \
  --user "$CB_USERNAME:$CB_PASSWORD" \
  --silent \
  "$PROTOCOL://$CLUSTER:$PORT/pools/nodes" | \
  jq -r '.nodes[] |
    select(.services | contains(["cbas"]) == true) |
    .hostname'
  )
do
  echo "-------------------------------------------------------"
  echo "$node Analytics Service Stats"
  echo "-------------------------------------------------------"
  # loop over each of the buckets
  # shellcheck disable=SC2068
  for bucket in ${buckets[@]}
  do
    echo "Bucket: $bucket"
    echo "======================================================="
    # get the analytics stats for the bucket
    curl \
      --user "$CB_USERNAME:$CB_PASSWORD" \
      --silent \
      "$PROTOCOL://$CLUSTER:$PORT/pools/default/buckets/@cbas-$bucket/nodes/$node/stats?zoom=$ZOOM" | \
      jq -r -c 'def roundit: .*100.0 + 0.5|floor/100.0 | .*100.0;
      .op.samples |
      "  cbas/all/failed_at_parser_records_count: " + (try (.["cbas/all/failed_at_parser_records_count"] | add | roundit/100.0 | tostring) catch "N/A") +
      "\n  cbas/all/failed_at_parser_records_count_total: " + (try (.["cbas/all/failed_at_parser_records_count_total"] | add / length | roundit/100.0 | tostring) catch "N/A") +
      "\n  cbas/all/incoming_records_count: " + (try (.["cbas/all/incoming_records_count"] | add | roundit/100.0 | tostring) catch "N/A") +
      "\n  cbas/all/incoming_records_count_total: " + (try (.["cbas/all/incoming_records_count_total"] | add / length | roundit/100.0 | tostring) catch "N/A") +
      "\n  cbas/failed_at_parser_records_count: " + (try (.["cbas/failed_at_parser_records_count"] | add | roundit/100.0 | tostring) catch "N/A") +
      "\n  cbas/failed_at_parser_records_count_total: " + (try (.["cbas/failed_at_parser_records_count_total"] | add / length | roundit/100.0 | tostring) catch "N/A") +
      "\n  cbas/incoming_records_count: " + (try (.["cbas/incoming_records_count"] | add | roundit/100.0 | tostring) catch "N/A") +
      "\n  cbas/incoming_records_count_total: " + (try (.["cbas/incoming_records_count_total"] | add / length | roundit/100.0 | tostring) catch "N/A")
      '
  done
done
