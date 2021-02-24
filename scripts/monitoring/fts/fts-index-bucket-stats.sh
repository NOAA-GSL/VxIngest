#!/usr/bin/env bash

# ***********************************************************************************************************
# index-stats.sh
#
#   Usage: ./fts-index-bucket-stats.sh [options]
#
#   This will output the fts index stats for a specific buckets in the cluster, and explicitly
#   reference each of the available stats, and will correctly sum() or average()
#   the appropriate properties
#
#   Options:
#
#     --cluster=<s>           The cluster address (default: localhost)
#     --bucket=<s>            The name of the bucket (default: default)
#     --username=<s>          Cluster Admin or RBAC username (default: Administrator)
#     --password=<s>          Cluster Admin or RBAC password (default: password)
#     --port=<s>              The port to use (default: 8091)
#     --protocol=<s>          The protocol to use (default: http)
#     --include-replicas=<b>  Whether or not to include replicas
#     --zoom=<s>              The option to sample minute, hour, day, week, year (default: minute)
# ***********************************************************************************************************

# set the defaults, these can all be overriden as environment variables or passed via the cli
CB_USERNAME=${CB_USERNAME:='Administrator'}
CB_PASSWORD=${CB_PASSWORD:='password'}
CLUSTER=${CLUSTER:='localhost'}
BUCKET=${BUCKET:='default'}
PORT=${PORT:='8091'}
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
    --bucket=*)
      BUCKET="${1#*=}"
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

# get the index stats for the bucket
curl \
  --user "$CB_USERNAME:$CB_PASSWORD" \
  --silent \
  "$PROTOCOL://$CLUSTER:$PORT/pools/default/buckets/@fts-$BUCKET/stats?zoom=$ZOOM" | \
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
          "total_queries","total_queries_error","total_queries_slow",
          "total_queries_timeout"
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
    "\n  doc_count: " + (try ($stats["fts\/" + . + "\/doc_count"] | tostring ) catch "N/A") +
    "\n  num_bytes_used_disk: " + (try ($stats["fts\/" + . + "\/num_bytes_used_disk"] | tostring) catch "N/A") +
    "\n  num_mutations_to_index: " + (try ($stats["fts\/" + . + "\/num_mutations_to_index"] | tostring) catch "N/A") +
    "\n  num_pindexes_actual: " + (try ($stats["fts\/" + . + "\/num_pindexes_actual"] | tostring) catch "N/A") +
    "\n  num_pindexes_target: " + (try ($stats["fts\/" + . + "\/num_pindexes_target"] | tostring) catch "N/A") +
    "\n  num_recs_to_persist: " + (try ($stats["fts\/" + . + "\/num_recs_to_persist"] | tostring) catch "N/A") +
    "\n  total_bytes_indexed: " + (try ($stats["fts\/" + . + "\/total_bytes_indexed"] | tostring) catch "N/A") +
    "\n  total_bytes_query_results: " + (try ($stats["fts\/" + . + "\/total_bytes_query_results"] | tostring) catch "N/A") +
    "\n  total_compaction_written_bytes: " + (try ($stats["fts\/" + . + "\/total_compaction_written_bytes"] | tostring) catch "N/A") +
    "\n  total_queries: " + (try ($stats["fts\/" + . + "\/total_queries"] | tostring) catch "N/A") +
    "\n  total_queries_error: " + (try ($stats["fts\/" + . + "\/total_queries_error"] | tostring) catch "N/A") +
    "\n  total_queries_timeout: " + (try ($stats["fts\/" + . + "\/total_queries_timeout"] | tostring) catch "N/A") +
    "\n  total_request_time: " + (try ($stats["fts\/" + . + "\/total_request_time"] | tostring) catch "N/A") +
    "\n  total_term_searchers: " + (try ($stats["fts\/" + . + "\/total_term_searchers"] | tostring) catch "N/A") +
    "\n"
  '
