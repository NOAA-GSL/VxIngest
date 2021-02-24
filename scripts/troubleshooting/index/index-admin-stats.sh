#!/usr/bin/env bash

# ***********************************************************************************************************
# index-admin-stats.sh
#
#   Usage: ./index-admin-stats.sh [options]
#
#   This will output the index admin stats for one or more nodes in the cluster.
#
#   *NOTE* The machine executing this script must be able to access port: 9102
#          on the index nodes the stats are being requested from
#
#   Options:
#
#     --cluster=<s>           The cluster address (default: localhost)
#     --username=<s>          Cluster Admin or RBAC username (default: Administrator)
#     --password=<s>          Cluster Admin or RBAC password (default: password)
#     --port=<s>              The port to use (default: 8091)
#     --protocol=<s>          The protocol to use (default: http)
#     --nodes=<s>             A comma-delimited list of index node hostnames/ips, all nodes are included by default (default: "*")
#     --buckets=<s>           A comma-delimited list of buckets, all buckets are included by default (default: "*")
#     --indexes=<s>           A comma-delimited list of indexes, all indexes are included by default (default: "*")
#     --partition=<b>         Boolean flag on whether or not to include the partition stats (default: false)
# ***********************************************************************************************************

# set the defaults, these can all be overriden as environment variables or passed via the cli
CB_USERNAME=${CB_USERNAME:='Administrator'}
CB_PASSWORD=${CB_PASSWORD:='password'}
CLUSTER=${CLUSTER:='localhost'}
PORT=${PORT:='8091'}
PROTOCOL=${PROTOCOL:='http'}
NODES=${NODES:='*'}
BUCKETS=${BUCKETS:='*'}
INDEXES=${INDEXES:='*'}
PARTITION=${PARTITION:='false'}

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
    --buckets=*)
      BUCKETS="${1#*=}"
      ;;
    --indexes=*)
      INDEXES="${1#*=}"
      ;;
    --partition=*)
      PARTITION="${1#*=}"
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

# if nodes is all, then get a list of index nodes from the cluster manager
if [[ "$NODES" == "*" ]]; then
  # get the index nodes
  NODES=$(curl \
    --user "$CB_USERNAME:$CB_PASSWORD" \
    --silent \
    --request GET \
    "$PROTOCOL://$CLUSTER:$PORT/pools/nodes" | \
    jq -r '.nodes[] |
      select(.services | contains(["index"]) == true) |
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
    --data pretty=false \
    --data skip_empty=false \
    "http://$node:9102/stats?partition=$PARTITION")

  # output the service level-stats
  echo "$stats" | \
    jq -r --arg node "$node" '
    "****************************************************************" +
    "\n*  " + $node + ":9102" +
    "\n****************************************************************" +
    "\n  cpu_utilization: " + (.cpu_utilization | tostring) +
    "\n  index_not_found_errcount: " + (.index_not_found_errcount | tostring) +
    "\n  indexer_state: " + (.indexer_state) +
    "\n  memory_free: " + (.memory_free | tostring) +
    "\n  memory_quota: " + (.memory_quota | tostring) +
    "\n  memory_rss: " + (.memory_rss | tostring) +
    "\n  memory_total: " + (.memory_total | tostring) +
    "\n  memory_total_storage: " + (.memory_total_storage | tostring) +
    "\n  memory_used: " + (.memory_used | tostring) +
    "\n  memory_used_queue: " + (.memory_used_queue | tostring) +
    "\n  memory_used_storage: " + (.memory_used_storage | tostring) +
    "\n  needs_restart: " + (.needs_restart | tostring) +
    "\n  num_connections: " + (.num_connections | tostring) +
    "\n  num_cpu_core: " + (.num_cpu_core | tostring) +
    "\n  storage_mode: " + (.storage_mode) +
    "\n  timings/stats_response: " + (.["timings/stats_response"]) +
    "\n  uptime: " + (.uptime) +
    "\n"
    '

  # output the bucket stats
  echo "$stats" | \
    jq -r --arg bucketList "$BUCKETS" --arg indexList "$INDEXES" --arg partition "$PARTITION" '. as $stats |
    $stats |
    # get the unique list of buckets
    (
      if($bucketList == "*") then
        [
          $stats | to_entries[] |
            select((.key | split(":") | length == 2)
            and
            (.key | test("^[A-Za-z].+"; ""))) |
          .key | split(":")[0]
          ] | sort | unique
      else
        $bucketList | split(",")
      end
    ) as $buckets |
    $stats | (
      [
        $stats | to_entries[] |
          select((.key | split(":") | length == 2)
          and
          (.key | test("^[A-Za-z].+"; "")))
        | .
      ] | from_entries
    ) as $bucket_stats |
    # loop over each bucket and output its stats
    $buckets[] | . as $bucket | $bucket |
    "\n----------------------------------------------------------------" +
    "\n|  Bucket: " + . +
    "\n________________________________________________________________" +
    "\n  mutation_queue_size: " + ($bucket_stats[. + ":mutation_queue_size"] | tostring) +
    "\n  num_mutations_queued: " + ($bucket_stats[. + ":num_mutations_queued"] | tostring) +
    "\n  num_nonalign_ts: " + ($bucket_stats[. + ":num_nonalign_ts"] | tostring) +
    "\n  num_rollbacks: " + ($bucket_stats[. + ":num_rollbacks"] | tostring) +
    "\n  timings/dcp_getseqs: " + ($bucket_stats[. + ":timings/dcp_getseqs"] | tostring) +
    "\n  ts_queue_size: " + ($bucket_stats[. + ":ts_queue_size"] | tostring) +
    "\n" +
    # get a unique list of indexes for the bucket and loop over them
    (
      $stats | (
        # only select keys who have 3 positions when split on ":" and the
        # first position in the key is the bucket name.  If partition == true
        # filter out non-partition entries to eliminate confusion. If a specific
        # list of indexes was passed, filter out indexes not in the list
        [
          . | to_entries[] | .key as $key | . | select(
            (
              $key | split(":") | length == 3
              and (.[0] == $bucket)
              and (
               ($partition == "true" and (.[1] | split(" ") | length > 1))
               or
               ($partition == "false")
              )
              and (
                ($indexList == "*")
                or
                ($indexList | split(",") | index($key | split(":")[1] | split(" ")[0]) >= 0)
              )
            )
          ) | .key | split(":")[1]
        ] | sort | unique
      )
    ) as $indexes |
    reduce $indexes[] as $index ("";
      . +=
        # partitions are listed just as indexes are but they have a space followed by a number denoting the partition
        "\n  Index: " + ($index | split(" ")[0])
          + (
            if($partition == "true" and ($index | split(" ") | length > 1)) then
              "   Partition: " + ($index | split(" ")[1] | tostring)
            else
              ""
            end
          ) +
        "\n  --------------------------------------------------------------" +
        "\n    avg_disk_bps: " + ($stats[$bucket + ":" + $index + ":avg_disk_bps"] | tostring) +
        "\n    avg_drain_rate: " + ($stats[$bucket + ":" + $index + ":avg_drain_rate"] | tostring) +
        "\n    avg_mutation_rate: " + ($stats[$bucket + ":" + $index + ":avg_mutation_rate"] | tostring) +
        "\n    avg_scan_latency: " + ($stats[$bucket + ":" + $index + ":avg_scan_latency"] | tostring) +
        "\n    avg_scan_rate: " + ($stats[$bucket + ":" + $index + ":avg_scan_rate"] | tostring) +
        "\n    avg_scan_request_alloc_latency: " + ($stats[$bucket + ":" + $index + ":avg_scan_request_alloc_latency"] | tostring) +
        "\n    avg_scan_request_init_latency: " + ($stats[$bucket + ":" + $index + ":avg_scan_request_init_latency"] | tostring) +
        "\n    avg_scan_request_latency: " + ($stats[$bucket + ":" + $index + ":avg_scan_request_latency"] | tostring) +
        "\n    avg_scan_wait_latency: " + ($stats[$bucket + ":" + $index + ":avg_scan_wait_latency"] | tostring) +
        "\n    avg_ts_interval: " + ($stats[$bucket + ":" + $index + ":avg_ts_interval"] | tostring) +
        "\n    avg_ts_items_count: " + ($stats[$bucket + ":" + $index + ":avg_ts_items_count"] | tostring) +
        "\n    build_progress: " + ($stats[$bucket + ":" + $index + ":build_progress"] | tostring) +
        "\n    cache_hit_percent: " + ($stats[$bucket + ":" + $index + ":cache_hit_percent"] | tostring) +
        "\n    cache_hits: " + ($stats[$bucket + ":" + $index + ":cache_hits"] | tostring) +
        "\n    cache_misses: " + ($stats[$bucket + ":" + $index + ":cache_misses"] | tostring) +
        "\n    client_cancel_errcount: " + ($stats[$bucket + ":" + $index + ":client_cancel_errcount"] | tostring) +
        "\n    data_size: " + ($stats[$bucket + ":" + $index + ":data_size"] | tostring) +
        "\n    delete_bytes: " + ($stats[$bucket + ":" + $index + ":delete_bytes"] | tostring) +
        "\n    disk_load_duration: " + ($stats[$bucket + ":" + $index + ":disk_load_duration"] | tostring) +
        "\n    disk_size: " + ($stats[$bucket + ":" + $index + ":disk_size"] | tostring) +
        "\n    disk_store_duration: " + ($stats[$bucket + ":" + $index + ":disk_store_duration"] | tostring) +
        "\n    flush_queue_size: " + ($stats[$bucket + ":" + $index + ":flush_queue_size"] | tostring) +
        "\n    frag_percent: " + ($stats[$bucket + ":" + $index + ":frag_percent"] | tostring) +
        "\n    get_bytes: " + ($stats[$bucket + ":" + $index + ":get_bytes"] | tostring) +
        "\n    insert_bytes: " + ($stats[$bucket + ":" + $index + ":insert_bytes"] | tostring) +
        "\n    items_count: " + ($stats[$bucket + ":" + $index + ":items_count"] | tostring) +
        "\n    last_rollback_time: " + ($stats[$bucket + ":" + $index + ":last_rollback_time"] | tostring) +
        "\n    memory_used: " + ($stats[$bucket + ":" + $index + ":memory_used"] | tostring) +
        "\n    not_ready_errcount: " + ($stats[$bucket + ":" + $index + ":not_ready_errcount"] | tostring) +
        "\n    num_commits: " + ($stats[$bucket + ":" + $index + ":num_commits"] | tostring) +
        "\n    num_compactions: " + ($stats[$bucket + ":" + $index + ":num_compactions"] | tostring) +
        "\n    num_completed_requests: " + ($stats[$bucket + ":" + $index + ":num_completed_requests"] | tostring) +
        "\n    num_completed_requests_aggr: " + ($stats[$bucket + ":" + $index + ":num_completed_requests_aggr"] | tostring) +
        "\n    num_completed_requests_range: " + ($stats[$bucket + ":" + $index + ":num_completed_requests_range"] | tostring) +
        "\n    num_docs_indexed: " + ($stats[$bucket + ":" + $index + ":num_docs_indexed"] | tostring) +
        "\n    num_docs_pending: " + ($stats[$bucket + ":" + $index + ":num_docs_pending"] | tostring) +
        "\n    num_docs_processed: " + ($stats[$bucket + ":" + $index + ":num_docs_processed"] | tostring) +
        "\n    num_docs_queued: " + ($stats[$bucket + ":" + $index + ":num_docs_queued"] | tostring) +
        "\n    num_flush_queued: " + ($stats[$bucket + ":" + $index + ":num_flush_queued"] | tostring) +
        "\n    num_items_flushed: " + ($stats[$bucket + ":" + $index + ":num_items_flushed"] | tostring) +
        "\n    num_items_restored: " + ($stats[$bucket + ":" + $index + ":num_items_restored"] | tostring) +
        "\n    num_last_snapshot_reply: " + ($stats[$bucket + ":" + $index + ":num_last_snapshot_reply"] | tostring) +
        "\n    num_requests: " + ($stats[$bucket + ":" + $index + ":num_requests"] | tostring) +
        "\n    num_requests_aggr: " + ($stats[$bucket + ":" + $index + ":num_requests_aggr"] | tostring) +
        "\n    num_requests_range: " + ($stats[$bucket + ":" + $index + ":num_requests_range"] | tostring) +
        "\n    num_rows_returned: " + ($stats[$bucket + ":" + $index + ":num_rows_returned"] | tostring) +
        "\n    num_rows_returned_aggr: " + ($stats[$bucket + ":" + $index + ":num_rows_returned_aggr"] | tostring) +
        "\n    num_rows_returned_range: " + ($stats[$bucket + ":" + $index + ":num_rows_returned_range"] | tostring) +
        "\n    num_rows_scanned: " + ($stats[$bucket + ":" + $index + ":num_rows_scanned"] | tostring) +
        "\n    num_rows_scanned_aggr: " + ($stats[$bucket + ":" + $index + ":num_rows_scanned_aggr"] | tostring) +
        "\n    num_rows_scanned_range: " + ($stats[$bucket + ":" + $index + ":num_rows_scanned_range"] | tostring) +
        "\n    num_snapshot_waiters: " + ($stats[$bucket + ":" + $index + ":num_snapshot_waiters"] | tostring) +
        "\n    num_snapshots: " + ($stats[$bucket + ":" + $index + ":num_snapshots"] | tostring) +
        "\n    progress_stat_time: " + ($stats[$bucket + ":" + $index + ":progress_stat_time"] | tostring) +
        "\n    recs_in_mem: " + ($stats[$bucket + ":" + $index + ":recs_in_mem"] | tostring) +
        "\n    recs_on_disk: " + ($stats[$bucket + ":" + $index + ":recs_on_disk"] | tostring) +
        "\n    resident_percent: " + ($stats[$bucket + ":" + $index + ":resident_percent"] | tostring) +
        "\n    scan_bytes_read: " + ($stats[$bucket + ":" + $index + ":scan_bytes_read"] | tostring) +
        "\n    scan_cache_hit_aggr: " + ($stats[$bucket + ":" + $index + ":scan_cache_hit_aggr"] | tostring) +
        "\n    scan_cache_hit_range: " + ($stats[$bucket + ":" + $index + ":scan_cache_hit_range"] | tostring) +
        "\n    scan_wait_duration: " + ($stats[$bucket + ":" + $index + ":scan_wait_duration"] | tostring) +
        "\n    since_last_snapshot: " + ($stats[$bucket + ":" + $index + ":since_last_snapshot"] | tostring) +
        "\n    timings/dcp_getseqs: " + ($stats[$bucket + ":" + $index + ":timings/dcp_getseqs"] | tostring) +
        "\n    timings/n1ql_expr_eval: " + ($stats[$bucket + ":" + $index + ":timings/n1ql_expr_eval"] | tostring) +
        "\n    timings/scan_pipeline_iterate: " + ($stats[$bucket + ":" + $index + ":timings/scan_pipeline_iterate"] | tostring) +
        "\n    timings/storage_clone_handle: " + ($stats[$bucket + ":" + $index + ":timings/storage_clone_handle"] | tostring) +
        "\n    timings/storage_commit: " + ($stats[$bucket + ":" + $index + ":timings/storage_commit"] | tostring) +
        "\n    timings/storage_del: " + ($stats[$bucket + ":" + $index + ":timings/storage_del"] | tostring) +
        "\n    timings/storage_get: " + ($stats[$bucket + ":" + $index + ":timings/storage_get"] | tostring) +
        "\n    timings/storage_info: " + ($stats[$bucket + ":" + $index + ":timings/storage_info"] | tostring) +
        "\n    timings/storage_iterator_next: " + ($stats[$bucket + ":" + $index + ":timings/storage_iterator_next"] | tostring) +
        "\n    timings/storage_meta_get: " + ($stats[$bucket + ":" + $index + ":timings/storage_meta_get"] | tostring) +
        "\n    timings/storage_meta_set: " + ($stats[$bucket + ":" + $index + ":timings/storage_meta_set"] | tostring) +
        "\n    timings/storage_new_iterator: " + ($stats[$bucket + ":" + $index + ":timings/storage_new_iterator"] | tostring) +
        "\n    timings/storage_persist_snapshot_create: " + ($stats[$bucket + ":" + $index + ":timings/storage_persist_snapshot_create"] | tostring) +
        "\n    timings/storage_set: " + ($stats[$bucket + ":" + $index + ":timings/storage_set"] | tostring) +
        "\n    timings/storage_snapshot_close: " + ($stats[$bucket + ":" + $index + ":timings/storage_snapshot_close"] | tostring) +
        "\n    timings/storage_snapshot_create: " + ($stats[$bucket + ":" + $index + ":timings/storage_snapshot_create"] | tostring) +
        "\n    total_scan_duration: " + ($stats[$bucket + ":" + $index + ":total_scan_duration"] | tostring) +
        "\n    total_scan_request_duration: " + ($stats[$bucket + ":" + $index + ":total_scan_request_duration"] | tostring) +
        "\n"
    )
    '
done
