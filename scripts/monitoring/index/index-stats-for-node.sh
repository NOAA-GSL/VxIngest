#!/usr/bin/env bash

# ***********************************************************************************************************
# index-stats-for-node.sh
#
#   Usage: ./index-stats-for-node.sh [options]
#
#   This will output the index stats for all buckets in the cluster, and explicitly
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
#     --include-replicas=<s>  Whether or not to include replicas
#     --node=<s>              A node hostname / ip in the cluster (default: localhost:8091)
#     --zoom=<s>              The option to sample minute, hour, day, week, year (default: minute)
# ***********************************************************************************************************

# set the defaults, these can all be overriden as environment variables or passed via the cli
CB_USERNAME=${CB_USERNAME:='Administrator'}
CB_PASSWORD=${CB_PASSWORD:='password'}
CLUSTER=${CLUSTER:='localhost'}
PORT=${PORT:='8091'}
NODE=${NODE:='localhost:8091'}
PROTOCOL=${PROTOCOL:='http'}
INCLUDE_REPLICAS=true
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
    --include-replicas=*)
      INCLUDE_REPLICAS="${1#*=}"
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
for bucket in $(curl \
  --user "$CB_USERNAME:$CB_PASSWORD" \
  --silent \
  "$PROTOCOL://$CLUSTER:$PORT/indexStatus" | \
  jq -r '[ .indexes[] | .bucket ] | sort | unique | .[]')
do
  echo ""
  echo "Bucket: $bucket"
  echo "================================================================"
  # get the index stats for the bucket
  curl \
    --user "$CB_USERNAME:$CB_PASSWORD" \
    --silent \
    "$PROTOCOL://$CLUSTER:$PORT/pools/default/buckets/@index-$bucket/nodes/$NODE/stats?zoom=$ZOOM" | \
    # 1. reduce the samples object, by looping over each property, only work with properties
    # who are index specific stat properties and either sum or average samples
    # 2. get all of the unique index keys
    # 3. loop over each index and output the stats
    jq -r --arg include_replicas "$INCLUDE_REPLICAS" '
      def roundit: .*100.0 + 0.5|floor/100.0 | .*100.0;
      reduce (.op.samples | to_entries[]) as {$key, $value} (
        {};
        if (
          $key | split("/") | length == 3
          and (
            $include_replicas == "true" or
            ($include_replicas == "false" and ($key | contains("replica ") | not))
            )
        ) then
          if ([
            "cache_hits","cache_misses","num_docs_indexed","num_docs_pending",
            "num_docs_pending+queued","num_docs_queued","num_requests",
            "num_rows_returned","scan_bytes_read","total_scan_duration"
          ] | .[] | contains($key | split("/") | .[2]) == true) then
            .[$key] += ($value | add)
          else
            .[$key] += ($value | add / length | roundit/100.0)
          end
        else
          .
        end
      ) | . as $stats |
      $stats | keys | map(split("/")[1]) | sort | unique as $indexes |
      $indexes | .[] |
      "Index: " + . +
      "\n----------------------------------------------------------------" +
      "\n  avg_item_size: " + (try ($stats["index\/" + . + "\/avg_item_size"] | tostring ) catch "N/A") +
      "\n  avg_scan_latency: " + (try ($stats["index\/" + . + "\/avg_scan_latency"] | tostring) catch "N/A") +
      "\n  cache_hits: " + (try ($stats["index\/" + . + "\/cache_hits"] | tostring) catch "N/A") +
      "\n  cache_miss_ratio: " + (try ($stats["index\/" + . + "\/cache_miss_ratio"] | tostring) catch "N/A") +
      "\n  cache_misses: " + (try ($stats["index\/" + . + "\/cache_misses"] | tostring) catch "N/A") +
      "\n  data_size: " + (try ($stats["index\/" + . + "\/data_size"] | tostring) catch "N/A") +
      "\n  disk_overhead_estimate: " + (try ($stats["index\/" + . + "\/disk_overhead_estimate"] | tostring) catch "N/A") +
      "\n  disk_size: " + (try ($stats["index\/" + . + "\/disk_size"] | tostring) catch "N/A") +
      "\n  frag_percent: " + (try ($stats["index\/" + . + "\/frag_percent"] | tostring) catch "N/A") +
      "\n  index_frag_percent: " + (try ($stats["index\/" + . + "\/index_frag_percent"] | tostring) catch "N/A") +
      "\n  index_resident_percent: " + (try ($stats["index\/" + . + "\/index_resident_percent"] | tostring) catch "N/A") +
      "\n  items_count: " + (try ($stats["index\/" + . + "\/items_count"] | tostring) catch "N/A") +
      "\n  memory_used: " + (try ($stats["index\/" + . + "\/memory_used"] | tostring) catch "N/A") +
      "\n  num_docs_indexed: " + (try ($stats["index\/" + . + "\/num_docs_indexed"] | tostring) catch "N/A") +
      "\n  num_docs_pending: " + (try ($stats["index\/" + . + "\/num_docs_pending"] | tostring) catch "N/A") +
      "\n  num_docs_pending+queued: " + (try ($stats["index\/" + . + "\/num_docs_pending+queued"] | tostring) catch "N/A") +
      "\n  num_docs_queued: " + (try ($stats["index\/" + . + "\/num_docs_queued"] | tostring) catch "N/A") +
      "\n  num_requests: " + (try ($stats["index\/" + . + "\/num_requests"] | tostring) catch "N/A") +
      "\n  num_rows_returned: " + (try ($stats["index\/" + . + "\/num_rows_returned"] | tostring) catch "N/A") +
      "\n  recs_in_mem: " + (try ($stats["index\/" + . + "\/recs_in_mem"] | tostring) catch "N/A") +
      "\n  recs_on_disk: " + (try ($stats["index\/" + . + "\/recs_on_disk"] | tostring) catch "N/A") +
      "\n  scan_bytes_read: " + (try ($stats["index\/" + . + "\/scan_bytes_read"] | tostring) catch "N/A") +
      "\n  avg_scan_latency: " + (try ($stats["index\/" + . + "\/avg_scan_latency"] | tostring) catch "N/A") +
      "\n  total_scan_duration: " + (try ($stats["index\/" + . + "\/total_scan_duration"] | tostring) catch "N/A") +
      "\n"
    '
done
