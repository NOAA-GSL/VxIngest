#!/usr/bin/env bash

# ***********************************************************************************************************
# index-export-csv.sh
#
#   Usage: ./index-export-csv.sh [options]
#
#   This will gather all of the indexes from a cluster, along with their definitions, placement and stats.
#
#   Options:
#
#     --cluster=<s>           The cluster address (default: localhost)
#     --username=<s>          Cluster Admin or RBAC username (default: Administrator)
#     --password=<s>          Cluster Admin or RBAC password (default: password)
#     --output=<s>            Destination to output the file (default: indexes-2018-10-10T09:27:11.csv)
#     --port=<s>              The port to use (default: 8091)
#     --protocol=<s>          The protocol to use (default: http)
#     --include-replicas=<s>  Whether or not to include replicas
#     --zoom=<s>              The option to sample minute, hour, day, week, year (default: minute)
# ***********************************************************************************************************

# set the defaults, these can all be overriden as environment variables or passed via the cli
CB_USERNAME=${CB_USERNAME:='Administrator'}
CB_PASSWORD=${CB_PASSWORD:='password'}
CLUSTER=${CLUSTER:='localhost'}
PORT=${PORT:='8091'}
PROTOCOL=${PROTOCOL:='http'}
OUTPUT_FILE="indexes-$(date +"%Y-%m-%dT%H:%M:%S").csv"
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
    --port=*)
      PORT="${1#*=}"
      ;;
    --protocol=*)
      PROTOCOL="${1#*=}"
      ;;
    --output=*)
      OUTPUT_FILE="${1#*=}"
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

# get all of the indexes and their definitions
indexes_status=$(curl \
  --user "$CB_USERNAME:$CB_PASSWORD" \
  --silent \
  "$PROTOCOL://$CLUSTER:$PORT/indexStatus" | \
  jq -r '.indexes | sort_by(.bucket)')

# get all of the buckets that have indexes
buckets=$(echo "$indexes_status" | jq -r '[ .[] | .bucket ] | unique | sort | .[]')

# initialize the output file
echo "Output File: $OUTPUT_FILE"
echo echo '"bucket","index","definition","hosts","avg_item_size","avg_scan_latency","cache_hits","cache_miss_ratio","cache_misses","data_size","disk_overhead_estimate","disk_size","frag_percent","index_frag_percent","index_resident_percent","items_count","memory_used","num_docs_indexed","num_docs_pending","num_docs_pending+queued","num_docs_queued","num_requests","num_rows_returned","recs_in_mem","recs_on_disk","scan_bytes_read","avg_scan_latency","total_scan_duration","curr_items","selectivity"' > "$OUTPUT_FILE"

# loop over each of the buckets
# shellcheck disable=SC2068
for bucket in ${buckets[@]}
do
  echo "Bucket: $bucket"
  # get the number of documents in the bucket (curr_items)
  curr_items=$(curl \
    --user "$CB_USERNAME:$CB_PASSWORD" \
    --silent \
    "$PROTOCOL://$CLUSTER:$PORT/pools/default/buckets/$bucket/stats?zoom=$ZOOM" | \
    jq -r -c 'def roundit: .*100.0 + 0.5|floor/100.0 | .*100.0;
      .op.samples.curr_items | (. | add / length | roundit/100 | tostring)')

  # get the index stats for the bucket
  stats=$(curl \
    --user "$CB_USERNAME:$CB_PASSWORD" \
    --silent \
    "$PROTOCOL://$CLUSTER:$PORT/pools/default/buckets/@index-$bucket/stats?zoom=$ZOOM" | \
    jq -r -c '.op.samples')

  # get the indexes for the bucket and replace spaces w/ %20
  if [[ $INCLUDE_REPLICAS == true ]]; then
    indexes=$(echo "$indexes_status" | jq -r ".[] |
      select(.bucket == \"$bucket\") |
      .index | sub(\" \"; \"%20\"; \"g\")" | sort | uniq)
  else
    indexes=$(echo "$indexes_status" | jq -r ".[] |
      select(.bucket == \"$bucket\" and (.index | test(\"^[^ ]+$\"))) |
      .index | sub(\" \"; \"%20\"; \"g\")" | sort | uniq)
  fi

  # get a total count of the number of indexes in the bucket
  indexCount=$(echo "$indexes" | wc -w)
  # initialize counter for the current indexes
  indexCounter=0
  # loop over each of the indexes in the bucket
  # shellcheck disable=SC2068
  for indexName in ${indexes[@]}
  do
    # $indexName was encoded so it could be looped, %20 needs to be replaced with a space
    indexName="${indexName//%20/ }" 
    # get the current index definition
    definition=$(echo "$indexes_status" | jq -r -c "map(select(.bucket == \"$bucket\"
      and .index == \"$indexName\")) | .[] | [ .definition ]")
    # get the current index hosts
    hosts=$(echo "$indexes_status" | jq -r -c ". | map(select(.[\"index\"] == \"$indexName\" and .bucket == \"$bucket\")) | .[] | .hosts | join(\",\")")
    # make sure definition and hosts were found
    if [[ ${definition} && ${hosts} ]]; then
      indexCounter=$((indexCounter + 1))
      tput cuu 1 && tput el && echo "Bucket: $bucket  Indexes: ($indexCounter / $indexCount) $indexName"
      #output to csv
      echo "$stats" | jq -r "def roundit: .*100.0 + 0.5|floor/100.0 | .*100.0;
        ([\"$bucket\"] | @csv) + \",\" +
        ([\"$indexName\"] | @csv) + \",\" +
        ($(echo "$definition" | jq -r -c '.') | @csv) + \",\" +
        ([\"$hosts\"] | @csv) + \",\" +
        (try (.[\"index\/$indexName\/avg_item_size\"] | (. | add / length | roundit/100.0 | tostring)) catch \"N/A\") + \",\" +
        (try (.[\"index\/$indexName\/avg_scan_latency\"] | (. | add / length | roundit/100.0 | tostring)) catch \"N/A\") + \",\" +
        (try (.[\"index\/$indexName\/cache_hits\"] | add | tostring) catch \"N/A\") + \",\" +
        (try (.[\"index\/$indexName\/cache_miss_ratio\"] | (. | add / length | roundit/100.0 | tostring)) catch \"N/A\") + \",\" +
        (try (.[\"index\/$indexName\/cache_misses\"] | add | tostring) catch \"N/A\") + \",\" +
        (try (.[\"index\/$indexName\/data_size\"] | (. | add / length | roundit/100.0 | tostring)) catch \"N/A\") + \",\" +
        (try (.[\"index\/$indexName\/disk_overhead_estimate\"] | (. | add / length | roundit/100.0 | tostring)) catch \"N/A\") + \",\" +
        (try (.[\"index\/$indexName\/disk_size\"] | (. | add / length | roundit/100.0 | tostring)) catch \"N/A\") + \",\" +
        (try (.[\"index\/$indexName\/frag_percent\"] | (. | add / length | roundit/100.0 | tostring)) catch \"N/A\") + \",\" +
        (try (.[\"index\/$indexName\/index_frag_percent\"] | (. | add / length | roundit/100.0 | tostring)) catch \"N/A\") + \",\" +
        (try (.[\"index\/$indexName\/index_resident_percent\"] | (. | add / length | roundit/100.0 | tostring)) catch \"N/A\") + \",\" +
        (try (.[\"index\/$indexName\/items_count\"] | (. | add / length | roundit/100.0 | tostring)) catch \"N/A\") + \",\" +
        (try (.[\"index\/$indexName\/memory_used\"] | (. | add / length | roundit/100.0 | tostring)) catch \"N/A\") + \",\" +
        (try (.[\"index\/$indexName\/num_docs_indexed\"] | add | tostring) catch \"N/A\") + \",\" +
        (try (.[\"index\/$indexName\/num_docs_pending\"] | add | tostring) catch \"N/A\") + \",\" +
        (try (.[\"index\/$indexName\/num_docs_pending+queued\"] | add | tostring) catch \"N/A\") + \",\" +
        (try (.[\"index\/$indexName\/num_docs_queued\"] | add | tostring) catch \"N/A\") + \",\" +
        (try (.[\"index\/$indexName\/num_requests\"] | add | tostring) catch \"N/A\") + \",\" +
        (try (.[\"index\/$indexName\/num_rows_returned\"] | add | tostring) catch \"N/A\") + \",\" +
        (try (.[\"index\/$indexName\/recs_in_mem\"] | (. | add / length | roundit/100.0 | tostring)) catch \"N/A\") + \",\" +
        (try (.[\"index\/$indexName\/recs_on_disk\"] | (. | add / length | roundit/100.0 | tostring)) catch \"N/A\") + \",\" +
        (try (.[\"index\/$indexName\/scan_bytes_read\"] | (. | add | roundit/100.0 | tostring)) catch \"N/A\") + \",\" +
        (try (.[\"index\/$indexName\/avg_scan_latency\"] | (. | add | roundit/100.0 | tostring)) catch \"N/A\") + \",\" +
        (try (.[\"index\/$indexName\/total_scan_duration\"] | add | tostring) catch \"N/A\") + \",\" +
        ([$curr_items] | @csv) + \",\" +
        (((.[\"index\/$indexName\/items_count\"] | (. | add / length | roundit/100)) / $curr_items) * 100 | roundit/100.00 | tostring) + \"%\"
        " >> "$OUTPUT_FILE"
    fi
  done
  tput cuu 1 && tput el && echo "Bucket: $bucket  Indexes: ($indexCounter / $indexCount)"
done
