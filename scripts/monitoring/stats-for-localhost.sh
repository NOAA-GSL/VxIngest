#!/usr/bin/env bash
# shellcheck disable=SC2163,SC2154,SC2155,SC2154
# set the defaults, these can all be overriden as environment variables or passed via the cli
SCRIPT=$(basename "$0")
CB_USERNAME=${CB_USERNAME:='Administrator'}
CB_PASSWORD=${CB_PASSWORD:='password'}
PORT=${PORT:='8091'}
PROTOCOL=${PROTOCOL:='http'}
ZOOM=${ZOOM:='minute'}

# ************************************************************************************
# Function: _usage
#   Output script usage information
# ************************************************************************************
function _usage
{
  echo "Description:"
  echo "  This script will output all of the available stats for the services that are running "
  echo "  on the node where the script was executed from."
  echo " "
  echo "Usage: ./$SCRIPT [options]"
  echo " "
  echo "Options:"
  echo "  --username <s>          Cluster Admin or RBAC username (default: Administrator)"
  echo "  --password <s>          Cluster Admin or RBAC password (default: password)"
  echo "  --protocol=<s>          The protocol to use http or https (default: http)"
  echo "  --port=<s>              The port to use (default: 8091)"
  echo "  --zoom <s>              The option to sample minute, hour, day, week, year (default: minute)"
  echo " "
  echo "Example:"
  echo "  ./$SCRIPT --username Administrator --password password --zoom=hour"
  exit 5
}

# ************************************************************************************
#  Function: _services
#   Get all of the services running on the localhost
# ************************************************************************************
function _services
{
  # get the services and hostname for the node where thisNode == true
  # and export the result as a variable i.e. hostname= and service=
  for s in $(curl \
    --user "$CB_USERNAME:$CB_PASSWORD" \
    --silent \
    "$PROTOCOL://localhost:$PORT/pools/default" | \
    jq -r ".nodes[] |
      select(.thisNode == true) |
      { hostname: .hostname, services: (.services | join(\",\")) } |
      to_entries |
      map(\"\(.key)=\(.value | tostring)\") |
      .[]
    "); do
      export "$s"
  done
}

# ************************************************************************************
#  Function: _heading
#   Outputs a heading
# ************************************************************************************
function _heading
{
  echo ""
  echo "****************************************************************"
  echo "*                      $1"
  echo "****************************************************************"
}

# ************************************************************************************
#  Function: _sub_heading
#   Outputs a sub heading
# ************************************************************************************
function _sub_heading
{
  echo ""
  echo "$1"
  echo "________________________________________________________________"
}

function _sub_heading_2
{
  echo ""
  echo "  $1"
  echo "----------------------------------------------------------------"
}

# ************************************************************************************
#  Function: _kv_stats
#   Retrieve all of the data stats for each bucket
# ************************************************************************************
function _kv_stats
{
  _heading "Data Stats"
  # loop over each of the buckets
  for bucket in $(curl \
    --user "$CB_USERNAME:$CB_PASSWORD" \
    --silent \
    "$PROTOCOL://localhost:$PORT/pools/default/buckets" | \
    jq -r '.[] | .name')
  do
    _sub_heading "Bucket: $bucket"
    # output the stats for the bucket
    curl \
      --user "$CB_USERNAME:$CB_PASSWORD" \
      --silent \
      "$PROTOCOL://localhost:$PORT/pools/default/buckets/$bucket/nodes/$hostname/stats?zoom=$ZOOM" | \
      jq -r -c 'def roundit: .*100.0 + 0.5|floor/100.0 | .*100.0;
      .op.samples |
      "  avg_active_timestamp_drift: " + (try (.avg_active_timestamp_drift | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  avg_bg_wait_time: " + (try (.avg_bg_wait_time | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  avg_disk_commit_time: " + (try (.avg_disk_commit_time | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  avg_disk_update_time: " + (try (.avg_disk_update_time | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  avg_replica_timestamp_drift: " + (try (.avg_replica_timestamp_drift | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  bg_wait_count: " + (try (.bg_wait_count | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  bg_wait_total: " + (try (.bg_wait_total | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  bytes_read: " + (try (.bytes_read | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  bytes_written: " + (try (.bytes_written | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  cas_badval: " + (try (.cas_badval | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  cas_hits: " + (try (.cas_hits | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  cas_misses: " + (try (.cas_misses | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  cmd_get: " + (try (.cmd_get | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  cmd_set: " + (try (.cmd_set | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  couch_docs_actual_disk_size: " + (try (.couch_docs_actual_disk_size | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  couch_docs_data_size: " + (try (.couch_docs_data_size | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  couch_docs_disk_size: " + (try (.couch_docs_disk_size | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  couch_docs_fragmentation: " + (try (.couch_docs_fragmentation | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  couch_spatial_data_size: " + (try (.couch_spatial_data_size | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  couch_spatial_disk_size: " + (try (.couch_spatial_disk_size | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  couch_spatial_ops: " + (try (.couch_spatial_ops | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  couch_total_disk_size: " + (try (.couch_total_disk_size | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  couch_views_actual_disk_size: " + (try (.couch_views_actual_disk_size | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  couch_views_data_size: " + (try (.couch_views_data_size | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  couch_views_disk_size: " + (try (.couch_views_disk_size | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  couch_views_fragmentation: " + (try (.couch_views_fragmentation | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  couch_views_ops: " + (try (.couch_views_ops | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  curr_connections: " + (try (.curr_connections | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  curr_items: " + (try (.curr_items | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  curr_items_tot: " + (try (.curr_items_tot | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  decr_hits: " + (try (.decr_hits | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  decr_misses: " + (try (.decr_misses | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  delete_hits: " + (try (.delete_hits | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  delete_misses: " + (try (.delete_misses | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  disk_commit_count: " + (try (.disk_commit_count | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  disk_commit_total: " + (try (.disk_commit_total | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  disk_update_count: " + (try (.disk_update_count | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  disk_update_total: " + (try (.disk_update_total | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  disk_write_queue: " + (try (.disk_write_queue | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_active_ahead_exceptions: " + (try (.ep_active_ahead_exceptions | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_active_hlc_drift: " + (try (.ep_active_hlc_drift | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_active_hlc_drift_count: " + (try (.ep_active_hlc_drift_count | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_bg_fetched: " + (try (.ep_bg_fetched | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_cache_miss_rate: " + (try (.ep_cache_miss_rate | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_clock_cas_drift_threshold_exceeded: " + (try (.ep_clock_cas_drift_threshold_exceeded | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_data_read_failed: " + (try (.ep_data_read_failed | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_data_write_failed: " + (try (.ep_data_write_failed | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_dcp_2i_backoff: " + (try (.ep_dcp_2i_backoff | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_dcp_2i_count: " + (try (.ep_dcp_2i_count | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_dcp_2i_items_remaining: " + (try (.ep_dcp_2i_items_remaining | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_dcp_2i_items_sent: " + (try (.ep_dcp_2i_items_sent | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_dcp_2i_producer_count: " + (try (.ep_dcp_2i_producer_count | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_dcp_2i_total_backlog_size: " + (try (.ep_dcp_2i_total_backlog_size | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_dcp_2i_total_bytes: " + (try (.ep_dcp_2i_total_bytes | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_dcp_cbas_backoff: " + (try (.ep_dcp_cbas_backoff | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_dcp_cbas_count: " + (try (.ep_dcp_cbas_count | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_dcp_cbas_items_remaining: " + (try (.ep_dcp_cbas_items_remaining | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_dcp_cbas_items_sent: " + (try (.ep_dcp_cbas_items_sent | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_dcp_cbas_producer_count: " + (try (.ep_dcp_cbas_producer_count | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_dcp_cbas_total_backlog_size: " + (try (.ep_dcp_cbas_total_backlog_size | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_dcp_cbas_total_bytes: " + (try (.ep_dcp_cbas_total_bytes | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_dcp_fts_backoff: " + (try (.ep_dcp_fts_backoff | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_dcp_fts_count: " + (try (.ep_dcp_fts_count | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_dcp_fts_items_remaining: " + (try (.ep_dcp_fts_items_remaining | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_dcp_fts_items_sent: " + (try (.ep_dcp_fts_items_sent | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_dcp_fts_producer_count: " + (try (.ep_dcp_fts_producer_count | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_dcp_fts_total_backlog_size: " + (try (.ep_dcp_fts_total_backlog_size | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_dcp_fts_total_bytes: " + (try (.ep_dcp_fts_total_bytes | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_dcp_other_backoff: " + (try (.ep_dcp_other_backoff | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_dcp_other_count: " + (try (.ep_dcp_other_count | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_dcp_other_items_remaining: " + (try (.ep_dcp_other_items_remaining | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_dcp_other_items_sent: " + (try (.ep_dcp_other_items_sent | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_dcp_other_producer_count: " + (try (.ep_dcp_other_producer_count | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_dcp_other_total_backlog_size: " + (try (.ep_dcp_other_total_backlog_size | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_dcp_other_total_bytes: " + (try (.ep_dcp_other_total_bytes | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_dcp_replica_backoff: " + (try (.ep_dcp_replica_backoff | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_dcp_replica_count: " + (try (.ep_dcp_replica_count | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_dcp_replica_items_remaining: " + (try (.ep_dcp_replica_items_remaining | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_dcp_replica_items_sent: " + (try (.ep_dcp_replica_items_sent | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_dcp_replica_producer_count: " + (try (.ep_dcp_replica_producer_count | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_dcp_replica_total_backlog_size: " + (try (.ep_dcp_replica_total_backlog_size | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_dcp_replica_total_bytes: " + (try (.ep_dcp_replica_total_bytes | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_dcp_views+indexes_backoff: " + (try (.["ep_dcp_views+indexes_backoff"] | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_dcp_views+indexes_count: " + (try (.["ep_dcp_views+indexes_count"] | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_dcp_views+indexes_items_remaining: " + (try (.["ep_dcp_views+indexes_items_remaining"] | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_dcp_views+indexes_items_sent: " + (try (.["ep_dcp_views+indexes_items_sent"] | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_dcp_views+indexes_producer_count: " + (try (.["ep_dcp_views+indexes_producer_count"] | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_dcp_views+indexes_total_backlog_size: " + (try (.["ep_dcp_views+indexes_total_backlog_size"] | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_dcp_views+indexes_total_bytes: " + (try (.["ep_dcp_views+indexes_total_bytes"] | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_dcp_views_backoff: " + (try (.ep_dcp_views_backoff | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_dcp_views_count: " + (try (.ep_dcp_views_count | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_dcp_views_items_remaining: " + (try (.ep_dcp_views_items_remaining | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_dcp_views_items_sent: " + (try (.ep_dcp_views_items_sent | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_dcp_views_producer_count: " + (try (.ep_dcp_views_producer_count | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_dcp_views_total_backlog_size: " + (try (.ep_dcp_views_total_backlog_size | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_dcp_views_total_bytes: " + (try (.ep_dcp_views_total_bytes | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_dcp_xdcr_backoff: " + (try (.ep_dcp_xdcr_backoff | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_dcp_xdcr_count: " + (try (.ep_dcp_xdcr_count | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_dcp_xdcr_items_remaining: " + (try (.ep_dcp_xdcr_items_remaining | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_dcp_xdcr_items_sent: " + (try (.ep_dcp_xdcr_items_sent | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_dcp_xdcr_producer_count: " + (try (.ep_dcp_xdcr_producer_count | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_dcp_xdcr_total_backlog_size: " + (try (.ep_dcp_xdcr_total_backlog_size | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_dcp_xdcr_total_bytes: " + (try (.ep_dcp_xdcr_total_bytes | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_diskqueue_drain: " + (try (.ep_diskqueue_drain | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_diskqueue_fill: " + (try (.ep_diskqueue_fill | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_diskqueue_items: " + (try (.ep_diskqueue_items | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_flusher_todo: " + (try (.ep_flusher_todo | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_item_commit_failed: " + (try (.ep_item_commit_failed | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_kv_size: " + (try (.ep_kv_size | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_max_size: " + (try (.ep_max_size | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_mem_high_wat: " + (try (.ep_mem_high_wat | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_mem_low_wat: " + (try (.ep_mem_low_wat | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_meta_data_memory: " + (try (.ep_meta_data_memory | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_num_non_resident: " + (try (.ep_num_non_resident | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_num_ops_del_meta: " + (try (.ep_num_ops_del_meta | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_num_ops_del_ret_meta: " + (try (.ep_num_ops_del_ret_meta | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_num_ops_get_meta: " + (try (.ep_num_ops_get_meta | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_num_ops_set_meta: " + (try (.ep_num_ops_set_meta | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_num_ops_set_ret_meta: " + (try (.ep_num_ops_set_ret_meta | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_num_value_ejects: " + (try (.ep_num_value_ejects | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_oom_errors: " + (try (.ep_oom_errors | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_ops_create: " + (try (.ep_ops_create | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_ops_update: " + (try (.ep_ops_update | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_overhead: " + (try (.ep_overhead | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_queue_size: " + (try (.ep_queue_size | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_replica_ahead_exceptions: " + (try (.ep_replica_ahead_exceptions | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_replica_hlc_drift: " + (try (.ep_replica_hlc_drift | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_replica_hlc_drift_count: " + (try (.ep_replica_hlc_drift_count | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_resident_items_rate: " + (try (.ep_resident_items_rate | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_tmp_oom_errors: " + (try (.ep_tmp_oom_errors | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ep_vb_total: " + (try (.ep_vb_total | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  evictions: " + (try (.evictions | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  get_hits: " + (try (.get_hits | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  get_misses: " + (try (.get_misses | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  hit_ratio: " + (try (.hit_ratio | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  incr_hits: " + (try (.incr_hits | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  incr_misses: " + (try (.incr_misses | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  misses: " + (try (.misses | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  ops: " + (try (.ops | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  vb_active_eject: " + (try (.vb_active_eject | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  vb_active_itm_memory: " + (try (.vb_active_itm_memory | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  vb_active_meta_data_memory: " + (try (.vb_active_meta_data_memory | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  vb_active_num: " + (try (.vb_active_num | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  vb_active_num_non_resident: " + (try (.vb_active_num_non_resident | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  vb_active_ops_create: " + (try (.vb_active_ops_create | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  vb_active_ops_update: " + (try (.vb_active_ops_update | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  vb_active_queue_age: " + (try (.vb_active_queue_age | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  vb_active_queue_drain: " + (try (.vb_active_queue_drain | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  vb_active_queue_fill: " + (try (.vb_active_queue_fill | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  vb_active_queue_size: " + (try (.vb_active_queue_size | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  vb_active_resident_items_ratio: " + (try (.vb_active_resident_items_ratio | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  vb_avg_active_queue_age: " + (try (.vb_avg_active_queue_age | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  vb_avg_pending_queue_age: " + (try (.vb_avg_pending_queue_age | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  vb_avg_replica_queue_age: " + (try (.vb_avg_replica_queue_age | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  vb_avg_total_queue_age: " + (try (.vb_avg_total_queue_age | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  vb_pending_curr_items: " + (try (.vb_pending_curr_items | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  vb_pending_eject: " + (try (.vb_pending_eject | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  vb_pending_itm_memory: " + (try (.vb_pending_itm_memory | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  vb_pending_meta_data_memory: " + (try (.vb_pending_meta_data_memory | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  vb_pending_num: " + (try (.vb_pending_num | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  vb_pending_num_non_resident: " + (try (.vb_pending_num_non_resident | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  vb_pending_ops_create: " + (try (.vb_pending_ops_create | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  vb_pending_ops_update: " + (try (.vb_pending_ops_update | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  vb_pending_queue_age: " + (try (.vb_pending_queue_age | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  vb_pending_queue_drain: " + (try (.vb_pending_queue_drain | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  vb_pending_queue_fill: " + (try (.vb_pending_queue_fill | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  vb_pending_queue_size: " + (try (.vb_pending_queue_size | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  vb_pending_resident_items_ratio: " + (try (.vb_pending_resident_items_ratio | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  vb_replica_curr_items: " + (try (.vb_replica_curr_items | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  vb_replica_eject: " + (try (.vb_replica_eject | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  vb_replica_itm_memory: " + (try (.vb_replica_itm_memory | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  vb_replica_meta_data_memory: " + (try (.vb_replica_meta_data_memory | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  vb_replica_num: " + (try (.vb_replica_num | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  vb_replica_num_non_resident: " + (try (.vb_replica_num_non_resident | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  vb_replica_ops_create: " + (try (.vb_replica_ops_create | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  vb_replica_ops_update: " + (try (.vb_replica_ops_update | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  vb_replica_queue_age: " + (try (.vb_replica_queue_age | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  vb_replica_queue_drain: " + (try (.vb_replica_queue_drain | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  vb_replica_queue_fill: " + (try (.vb_replica_queue_fill | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  vb_replica_queue_size: " + (try (.vb_replica_queue_size | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  vb_replica_resident_items_ratio: " + (try (.vb_replica_resident_items_ratio | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  vb_total_queue_age: " + (try (.vb_total_queue_age | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
      "\n  xdc_ops: " + (try (.xdc_ops | (. | add / length | roundit/100.0 | tostring)) catch "N/A")
      '
  done

  # if the data services is running, attempt to get the XDCR stats
  _xdcr_stats
}

# ************************************************************************************
#  Function: _index_stats
#   Retrieve all of the index stats for each bucket
# ************************************************************************************
function _index_stats
{
  _heading "Index Stats"

  # get the index service stats for the node
  _sub_heading "Index Service"
  curl \
    --user "$CB_USERNAME:$CB_PASSWORD" \
    --silent \
    "$PROTOCOL://localhost:$PORT/pools/default/buckets/@index/nodes/$hostname/stats?zoom=$ZOOM" | \
    jq -r 'def roundit: .*100.0 + 0.5|floor/100.0 | .*100.0;
    .op.samples |
    "index_memory_quota: " + (try (.index_memory_quota | add / length | roundit/100.0 | tostring) catch "N/A") +
    "\nindex_memory_used: " + (try (.index_memory_used | add / length | roundit/100.0 | tostring) catch "N/A") +
    "\nindex_ram_percent: " + (try (.index_ram_percent | add / length | roundit/100.0 | tostring) catch "N/A") +
    "\nindex_remaining_ram: " + (try (.index_remaining_ram | add / length | roundit/100.0 | tostring) catch "N/A") +
    "\n"
    '

    # loop over each of the buckets
    for bucket in $(curl \
      --user "$CB_USERNAME:$CB_PASSWORD" \
      --silent \
      "$PROTOCOL://$CLUSTER:$PORT/indexStatus" | \
      jq -r '[ .indexes[] | .bucket ] | sort | unique | .[]')
    do
      _sub_heading "Bucket: $bucket"
      # get the index and aggregate stats for the bucket
      local index_stats=$(curl \
        --user "$CB_USERNAME:$CB_PASSWORD" \
        --silent \
        "$PROTOCOL://localhost:$PORT/pools/default/buckets/@index-$bucket/nodes/$hostname/stats?zoom=$ZOOM")

      # output the index aggregate stats
      _sub_heading_2 "Index Aggregate Stats"
      echo "$index_stats" | \
      jq -r '.op.samples |
        "  cache_hits: " + (try (.["index/cache_hits"] | add | tostring) catch "N/A") +
        "\n  cache_misses: " + (try (.["index/cache_misses"] | add | tostring) catch "N/A") +
        "\n  data_size: " + (try (.["index/data_size"] | add | tostring) catch "N/A") +
        "\n  disk_overhead_estimate: " + (try (.["index/disk_overhead_estimate"] | add / length | tostring) catch "N/A") +
        "\n  disk_size: " + (try (.["index/disk_size"] | add | tostring) catch "N/A") +
        "\n  frag_percent: " + (try (.["index/frag_percent"] | add / length | tostring) catch "N/A") +
        "\n  fragmentation: " + (try (.["index/fragmentation"] | add / length | tostring) catch "N/A") +
        "\n  items_count: " + (try (.["index/items_count"] | add / length | tostring) catch "N/A") +
        "\n  memory_used: " + (try (.["index/memory_used"] | add / length | tostring) catch "N/A") +
        "\n  num_docs_indexed: " + (try (.["index/num_docs_indexed"] | add | tostring) catch "N/A") +
        "\n  num_docs_pending: " + (try (.["index/num_docs_pending"] | add | tostring) catch "N/A") +
        "\n  num_docs_queued: " + (try (.["index/num_docs_queued"] | add | tostring) catch "N/A") +
        "\n  num_requests: " + (try (.["index/num_requests"] | add | tostring) catch "N/A") +
        "\n  num_rows_returned: " + (try (.["index/num_rows_returned"] | add | tostring) catch "N/A") +
        "\n  recs_in_mem: " + (try (.["index/recs_in_mem"] | add | tostring) catch "N/A") +
        "\n  recs_on_disk: " + (try (.["index/recs_on_disk"] | add | tostring) catch "N/A") +
        "\n  scan_bytes_read: " + (try (.["index/scan_bytes_read"] | add | tostring) catch "N/A") +
        "\n  total_scan_duration: " + (try (.["index/total_scan_duration"] | add | tostring) catch "N/A")
      '

      # output the individual index stats
      _sub_heading_2 "Per Index Stats"
      echo "$index_stats" | \
        # 1. reduce the samples object, by looping over each property, only work with properties
        # who are index specific stat properties and either sum or average samples
        # 2. get all of the unique index keys
        # 3. loop over each index and output the stats
        jq -r '
          def roundit: .*100.0 + 0.5|floor/100.0 | .*100.0;
          reduce (.op.samples | to_entries[]) as {$key, $value} (
            {};
            if ($key | split("/") | length == 3) then
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
          "\n  Index: " + . +
          "\n  --------------------------------------------------------------" +
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
          "\n  total_scan_duration: " + (try ($stats["index\/" + . + "\/total_scan_duration"] | tostring) catch "N/A")
        '
    done
}

# ************************************************************************************
#  Function: _n1ql_stats
#   Retrieve all of the n1ql stats for each bucket
# ************************************************************************************
function _n1ql_stats
{
  _heading "Query Stats"
  curl \
    --user "$CB_USERNAME:$CB_PASSWORD" \
    --silent \
    "$PROTOCOL://localhost:$PORT/pools/default/buckets/@query/nodes/$hostname/stats?zoom=$ZOOM" | \
    jq -r 'def roundit: .*100.0 + 0.5|floor/100.0 | .*100.0;
    .op.samples |
    "  query_avg_req_time: " + (try (.query_avg_req_time | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
    "\n  query_avg_svc_time: " + (try (.query_avg_svc_time | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
    "\n  query_avg_response_size: " + (try (.query_avg_response_size | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
    "\n  query_avg_result_count: " + (try (.query_avg_result_count | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
    "\n  query_active_requests: " + (try (.query_active_requests | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
    "\n  query_errors: " + (try (.query_errors | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
    "\n  query_invalid_requests: " + (try (.query_invalid_requests | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
    "\n  query_queued_requests: " + (try (.query_queued_requests | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
    "\n  query_request_time: " + (try (.query_request_time | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
    "\n  query_requests: " + (try (.query_requests | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
    "\n  query_requests_1000ms: " + (try (.query_requests_1000ms | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
    "\n  query_requests_250ms: " + (try (.query_requests_250ms | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
    "\n  query_requests_5000ms: " + (try (.query_requests_5000ms | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
    "\n  query_requests_500ms: " + (try (.query_requests_500ms | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
    "\n  query_result_count: " + (try (.query_result_count | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
    "\n  query_result_size: " + (try (.query_result_size | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
    "\n  query_selects: " + (try (.query_selects | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
    "\n  query_service_time: " + (try (.query_service_time | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
    "\n  query_warnings: " + (try (.query_warnings | (. | add / length | roundit/100.0 | tostring)) catch "N/A")
    '
}

# ************************************************************************************
#  Function: _fts_stats
#   Retrieve all of the fts stats for each bucket
# ************************************************************************************
function _fts_stats
{
  _heading "FTS Stats"
  _sub_heading "FTS Service Stats"
  curl \
    --user "$CB_USERNAME:$CB_PASSWORD" \
    --silent \
    "$PROTOCOL://localhost:$PORT/pools/default/buckets/@fts/nodes/$hostname/stats?zoom=$ZOOM" | \
    jq -r 'def roundit: .*100.0 + 0.5|floor/100.0 | .*100.0;
    .op.samples |
    "  fts_num_bytes_used_ram: " + (try (.fts_num_bytes_used_ram | add / length | roundit/100.0 | tostring) catch "N/A")
    '
  # loop over each of the buckets
  for bucket in $(curl \
    --user "$CB_USERNAME:$CB_PASSWORD" \
    --silent \
    "$PROTOCOL://localhost:$PORT/_p/fts/api/index" | \
    jq -r '.indexDefs.indexDefs | [ to_entries[] | .value.sourceName ] | sort | unique | .[]')
  do
    _sub_heading "Bucket: $bucket"
    # get the index stats for the bucket
    local fts_stats=$(curl \
      --user "$CB_USERNAME:$CB_PASSWORD" \
      --silent \
      "$PROTOCOL://localhost:$PORT/pools/default/buckets/@fts-$bucket/nodes/$hostname/stats?zoom=$ZOOM")

    _sub_heading_2 "FTS Index Aggregate Stats"
    echo "$fts_stats" | \
    jq -r '.op.samples |
      "  doc_count: " + (try (.["fts/doc_count"] | add | tostring) catch "N/A") +
      "\n  num_bytes_used_disk: " + (try (.["fts/num_bytes_used_disk"] | add / length | tostring) catch "N/A") +
      "\n  num_mutations_to_index: " + (try (.["fts/num_mutations_to_index"] | add | tostring) catch "N/A") +
      "\n  num_pindexes_actual: " + (try (.["fts/num_pindexes_actual"] | add | tostring) catch "N/A") +
      "\n  num_pindexes_target: " + (try (.["fts/num_pindexes_target"] | add | tostring) catch "N/A") +
      "\n  num_recs_to_persist: " + (try (.["fts/num_recs_to_persist"] | add | tostring) catch "N/A") +
      "\n  total_bytes_indexed: " + (try (.["fts/total_bytes_indexed"] | add | tostring) catch "N/A") +
      "\n  total_bytes_query_results: " + (try (.["fts/total_bytes_query_results"] | add | tostring) catch "N/A") +
      "\n  total_compaction_written_bytes: " + (try (.["fts/total_compaction_written_bytes"] | add | tostring) catch "N/A") +
      "\n  total_queries: " + (try (.["fts/total_queries"] | add | tostring) catch "N/A") +
      "\n  total_queries_error: " + (try (.["fts/total_queries_error"] | add | tostring) catch "N/A") +
      "\n  total_queries_slow: " + (try (.["fts/total_queries_slow"] | add | tostring) catch "N/A") +
      "\n  total_queries_timeout: " + (try (.["fts/total_queries_timeout"] | add | tostring) catch "N/A") +
      "\n  total_request_time: " + (try (.["fts/total_request_time"] | add | tostring) catch "N/A") +
      "\n  total_term_searchers: " + (try (.["fts/total_term_searchers"] | add | tostring) catch "N/A")
    '

    _sub_heading_2 "Per FTS Index Stats"
    # output the per index stats
    echo "$fts_stats" | \
      # 1. reduce the samples object, by looping over each property, only work with properties
      # who are index specific stat properties and either sum or average samples
      # 2. get all of the unique index keys
      # 3. loop over each index and output the stats
      jq -r --arg bucket "$bucket" '
        def roundit: .*100.0 + 0.5|floor/100.0 | .*100.0;
        reduce (.op.samples | to_entries[]) as {$key, $value} (
          {};
          if ($key | split("/") | length == 3) then
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
        "\n  Index: " + . +
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
  done
}

# ************************************************************************************
#  Function: _eventing_stats
#   Retrieve all of the eventing stats for each bucket
# ************************************************************************************
function _eventing_stats
{
  _heading "Eventing Stats"
  _sub_heading "Eventing Service Stats"
  # get the eventing stats
  local eventing_stats=$(curl \
    --user "$CB_USERNAME:$CB_PASSWORD" \
    --silent \
    "$PROTOCOL://localhost:$PORT/pools/default/buckets/@eventing/nodes/$hostname/stats?zoom=$ZOOM" | jq -r 'def roundit: .*100.0 + 0.5|floor/100.0 | .*100.0;
    .op.samples')

  # output service stats
  echo "$eventing_stats" | jq -r '. as $stats |
    "  bucket_op_exception_count: " + (try ($stats["eventing/bucket_op_exception_count"] | add | tostring) catch "N/A") +
    "\n  checkpoint_failure_count: " + (try ($stats["eventing/checkpoint_failure_count"] | add | tostring) catch "N/A") +
    "\n  dcp_backlog: " + (try ($stats["eventing/dcp_backlog"] | add |tostring) catch "N/A") +
    "\n  failed_count: " + (try ($stats["eventing/failed_count"] | add | tostring) catch "N/A") +
    "\n  n1ql_op_exception_count: " + (try ($stats["eventing/n1ql_op_exception_count"] | add | tostring) catch "N/A") +
    "\n  on_delete_failure: " + (try ($stats["eventing/on_delete_failure"] | add / length | tostring) catch "N/A") +
    "\n  on_delete_success: " + (try ($stats["eventing/on_delete_success"] | add / length | tostring) catch "N/A") +
    "\n  on_update_failure: " + (try ($stats["eventing/on_update_failure"] | add / length | tostring) catch "N/A") +
    "\n  on_update_success: " + (try ($stats["eventing/on_update_success"] | add / length | tostring) catch "N/A") +
    "\n  processed_count: " + (try ($stats["eventing/processed_count"] | add / length | tostring) catch "N/A") +
    "\n  timeout_count: " + (try ($stats["eventing/timeout_count"] | add | tostring) catch "N/A") +
    "\n"
  '

  _sub_heading "Per Function Stats"
  # output function stats
  echo "$eventing_stats" | jq -r '. as $stats
    | $stats | [
      keys | .[] | select(. | split("/") | length == 3) | split("/")[1]
    ] | sort | unique as $funcs
    | $funcs | .[] |
    "\n  Function: " + . +
    "\n----------------------------------------------------------------" +
    "\n  bucket_op_exception_count: " + (try ($stats["eventing/" + . + "/bucket_op_exception_count"] | add | tostring) catch "N/A") +
    "\n  checkpoint_failure_count: " + (try ($stats["eventing/" + . + "/checkpoint_failure_count"] | add | tostring) catch "N/A") +
    "\n  dcp_backlog: " + (try ($stats["eventing/" + . + "/dcp_backlog"] | add | tostring) catch "N/A") +
    "\n  failed_count: " + (try ($stats["eventing/" + . + "/failed_count"] | add | tostring) catch "N/A") +
    "\n  n1ql_op_exception_count: " + (try ($stats["eventing/" + . + "/n1ql_op_exception_count"] | add | tostring) catch "N/A") +
    "\n  on_delete_failure: " + (try ($stats["eventing/" + . + "/on_delete_failure"] | add / length | tostring) catch "N/A") +
    "\n  on_delete_success: " + (try ($stats["eventing/" + . + "/on_delete_success"] | add / length  | tostring) catch "N/A") +
    "\n  on_update_failure: " + (try ($stats["eventing/" + . + "/on_update_failure"] | add / length | tostring) catch "N/A") +
    "\n  on_update_success: " + (try ($stats["eventing/" + . + "/on_update_success"] | add / length | tostring) catch "N/A") +
    "\n  processed_count: " + (try ($stats["eventing/" + . + "/processed_count"] | add / length | tostring) catch "N/A") +
    "\n  timeout_count: " + (try ($stats["eventing/" + . + "/timeout_count"] | add | tostring) catch "N/A")'
}

# ************************************************************************************
#  Function: _cbas_stats
#   Retrieve all of the analytics stats for each bucket
# ************************************************************************************
function _cbas_stats
{
  _heading "Analytics Stats"

  _sub_heading "Analytics Service"
  curl \
    --user "$CB_USERNAME:$CB_PASSWORD" \
    --silent \
    "$PROTOCOL://localhost:$PORT/pools/default/buckets/@cbas/nodes/$hostname/stats?zoom=$ZOOM" | \
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

  # loop over each of the buckets with analytics
  for bucket in $(curl \
    --user "$CB_USERNAME:$CB_PASSWORD" \
    --silent \
    --data-urlencode "statement=SELECT Name FROM \`Metadata\`.\`Bucket\`;" \
    "$PROTOCOL://$CLUSTER:8095/analytics/service" | \
    jq -r '.results[].Name')
  do
    _sub_heading "Bucket: $bucket"
    # get the analytics stats for the bucket
    curl \
      --user "$CB_USERNAME:$CB_PASSWORD" \
      --silent \
      "$PROTOCOL://localhost:$PORT/pools/default/buckets/@cbas-$bucket/nodes/$hostname/stats?zoom=$ZOOM" | \
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
}

# ************************************************************************************
#  Function: _xdcr_stats
#   Retrieve all of the xdcr stats for each bucket
# ************************************************************************************
function _xdcr_stats
{
  _heading "XDCR Stats"

  local remoteClusters
  remoteClusters=$(curl \
    --user "$CB_USERNAME:$CB_PASSWORD" \
    --silent \
    "$PROTOCOL://$CLUSTER:$PORT/pools/default/remoteClusters" | \
    jq -r -c 'map({
      (.uuid | tostring): {
        "name": .name,
        "hostname": .hostname
      }
    }) | add')
  local bucket
  # loop over each of the buckets that have xdcr replications
  for bucket in $(curl \
    --user "$CB_USERNAME:$CB_PASSWORD" \
    --silent \
    $PROTOCOL://localhost:$PORT/pools/default/tasks | \
    jq -r '[ .[] | select(.type == "xdcr") | .source ] | sort | unique | .[]')
  do
    _sub_heading "Bucket: $bucket"
    # get the xdcr stats for the bucket
    curl \
      --user "$CB_USERNAME:$CB_PASSWORD" \
      --silent \
      "$PROTOCOL://localhost:$PORT/pools/default/buckets/@xdcr-$bucket/nodes/$hostname/stats?zoom=$ZOOM" | \
      # 1. reduce the samples object, by looping over each property, only work with properties
      # who are index specific stat properties and either sum or average samples
      # 2. get all of the unique index keys
      # 3. loop over each replication and output the stats
      jq -r --argjson remote_clusters "$remoteClusters" '
        def roundit: .*100.0 + 0.5|floor/100.0 | .*100.0;
        reduce (.op.samples | to_entries[]) as {$key, $value} (
          {};
          if (
            $key | split("/") | length == 5
          ) then
            if ([
              "docs_checked","docs_failed_cr_source","docs_filtered",
              "docs_opt_repd","docs_processed","docs_received_from_dcp",
              "docs_rep_queue","docs_written","expiry_docs_written",
              "expiry_failed_cr_source","expiry_filtered","expiry_received_from_dcp",
              "set_docs_written","set_failed_cr_source","set_filtered",
              "set_received_from_dcp"
            ] | .[] | contains($key | split("/") | .[4]) == true) then
              .[$key] += ($value | add)
            else
              .[$key] += ($value | add / length | roundit/100.0)
            end
          else
            .
          end
        ) | . as $stats |
        $stats | keys | map(split("/") | del(.[0, 4]) | join("/")) | sort | unique as $replications |
        $replications | .[] |
        (. | split("/")[0]) as $replicationId |
        (. | split("/")[1]) as $sourceBucket |
        (. | split("/")[2]) as $destBucket |
        "\nReplication: " + . +
        ($remote_clusters | .[($replicationId | tostring)] |
          "\nRemote Cluster: " + .name +
          "\nHostname: " + .hostname
        ) +
        "\nSource Bucket: " + $sourceBucket +
        "\nDestination Bucket: " + $destBucket +
        "\n----------------------------------------------------------------" +
        "\n  bandwidth_usage: " + (try ($stats["replications\/" + . + "\/bandwidth_usage"] | tostring ) catch "N/A") +
        "\n  changes_left: " + (try ($stats["replications\/" + . + "\/changes_left"] | tostring) catch "N/A") +
        "\n  data_replicated: " + (try ($stats["replications\/" + . + "\/data_replicated"] | tostring) catch "N/A") +
        "\n  dcp_datach_length: " + (try ($stats["replications\/" + . + "\/dcp_datach_length"] | tostring) catch "N/A") +
        "\n  dcp_dispatch_time: " + (try ($stats["replications\/" + . + "\/dcp_dispatch_time"] | tostring) catch "N/A") +
        "\n  deletion_docs_written: " + (try ($stats["replications\/" + . + "\/deletion_docs_written"] | tostring) catch "N/A") +
        "\n  deletion_failed_cr_source: " + (try ($stats["replications\/" + . + "\/deletion_failed_cr_source"] | tostring) catch "N/A") +
        "\n  deletion_filtered: " + (try ($stats["replications\/" + . + "\/deletion_filtered"] | tostring) catch "N/A") +
        "\n  deletion_received_from_dcp: " + (try ($stats["replications\/" + . + "\/deletion_received_from_dcp"] | tostring) catch "N/A") +
        "\n  docs_checked: " + (try ($stats["replications\/" + . + "\/docs_checked"] | tostring) catch "N/A") +
        "\n  docs_failed_cr_source: " + (try ($stats["replications\/" + . + "\/docs_failed_cr_source"] | tostring) catch "N/A") +
        "\n  docs_filtered: " + (try ($stats["replications\/" + . + "\/docs_filtered"] | tostring) catch "N/A") +
        "\n  docs_opt_repd: " + (try ($stats["replications\/" + . + "\/docs_opt_repd"] | tostring) catch "N/A") +
        "\n  docs_processed: " + (try ($stats["replications\/" + . + "\/docs_processed"] | tostring) catch "N/A") +
        "\n  docs_received_from_dcp: " + (try ($stats["replications\/" + . + "\/docs_received_from_dcp"] | tostring) catch "N/A") +
        "\n  docs_rep_queue: " + (try ($stats["replications\/" + . + "\/docs_rep_queue"] | tostring) catch "N/A") +
        "\n  docs_written: " + (try ($stats["replications\/" + . + "\/docs_written"] | tostring) catch "N/A") +
        "\n  expiry_docs_written: " + (try ($stats["replications\/" + . + "\/expiry_docs_written"] | tostring) catch "N/A") +
        "\n  expiry_failed_cr_source: " + (try ($stats["replications\/" + . + "\/expiry_failed_cr_source"] | tostring) catch "N/A") +
        "\n  expiry_filtered: " + (try ($stats["replications\/" + . + "\/expiry_filtered"] | tostring) catch "N/A") +
        "\n  expiry_received_from_dcp: " + (try ($stats["replications\/" + . + "\/expiry_received_from_dcp"] | tostring) catch "N/A") +
        "\n  num_checkpoints: " + (try ($stats["replications\/" + . + "\/num_checkpoints"] | tostring) catch "N/A") +
        "\n  num_failedckpts: " + (try ($stats["replications\/" + . + "\/num_failedckpts"] | tostring) catch "N/A") +
        "\n  percent_completeness: " + (try ($stats["replications\/" + . + "\/percent_completeness"] | tostring) catch "N/A") +
        "\n  rate_doc_checks: " + (try ($stats["replications\/" + . + "\/rate_doc_checks"] | tostring) catch "N/A") +
        "\n  rate_doc_opt_repd: " + (try ($stats["replications\/" + . + "\/rate_doc_opt_repd"] | tostring) catch "N/A") +
        "\n  rate_received_from_dcp: " + (try ($stats["replications\/" + . + "\/rate_received_from_dcp"] | tostring) catch "N/A") +
        "\n  rate_replicated: " + (try ($stats["replications\/" + . + "\/rate_replicated"] | tostring) catch "N/A") +
        "\n  resp_wait_time: " + (try ($stats["replications\/" + . + "\/resp_wait_time"] | tostring) catch "N/A") +
        "\n  set_docs_written: " + (try ($stats["replications\/" + . + "\/set_docs_written"] | tostring) catch "N/A") +
        "\n  set_failed_cr_source: " + (try ($stats["replications\/" + . + "\/set_failed_cr_source"] | tostring) catch "N/A") +
        "\n  set_filtered: " + (try ($stats["replications\/" + . + "\/set_filtered"] | tostring) catch "N/A") +
        "\n  set_received_from_dcp: " + (try ($stats["replications\/" + . + "\/set_received_from_dcp"] | tostring) catch "N/A") +
        "\n  size_rep_queue: " + (try ($stats["replications\/" + . + "\/size_rep_queue"] | tostring) catch "N/A") +
        "\n  throttle_latency: " + (try ($stats["replications\/" + . + "\/throttle_latency"] | tostring) catch "N/A") +
        "\n  time_committing: " + (try ($stats["replications\/" + . + "\/time_committing"] | tostring) catch "N/A") +
        "\n  wtavg_docs_latency: " + (try ($stats["replications\/" + . + "\/wtavg_docs_latency"] | tostring) catch "N/A") +
        "\n  wtavg_meta_latency: " + (try ($stats["replications\/" + . + "\/wtavg_meta_latency"] | tostring) catch "N/A")
      '
  done
}

# ************************************************************************************
#  Function: _system_stats
#   Retrieve all of the system stats for each bucket
# ************************************************************************************
function _system_stats
{
  _heading "System Stats"
  curl \
    --user "$CB_USERNAME:$CB_PASSWORD" \
    --silent \
    "$PROTOCOL://localhost:$PORT/pools/default/buckets/@system/nodes/$hostname/stats?zoom=$ZOOM" | \
    jq -r -c 'def roundit: .*100.0 + 0.5|floor/100.0 | .*100.0;
    .op.samples |
    "  checkpoint_failure_count: " + (try (.cpu_local_ms | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
    "\n  cpu_utilization_rate: " + (try (.cpu_utilization_rate | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
    "\n  hibernated_requests: " + (try (.hibernated_requests | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
    "\n  hibernated_waked: " + (try (.hibernated_waked | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
    "\n  mem_actual_free: " + (try (.mem_actual_free | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
    "\n  mem_actual_used: " + (try (.mem_actual_used | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
    "\n  mem_free: " + (try (.mem_free | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
    "\n  mem_total: " + (try (.mem_total | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
    "\n  mem_used_sys: " + (try (.mem_used_sys | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
    "\n  rest_requests: " + (try (.rest_requests | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
    "\n  swap_total: " + (try (.swap_total | (. | add / length | roundit/100.0 | tostring)) catch "N/A") +
    "\n  swap_used: " + (try (.swap_used | (. | add / length | roundit/100.0 | tostring)) catch "N/A")
    '
}

# start of execution
# ------------------------

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
    --help )
      _usage
      ;;
    *)
      echo "ERROR : Invalid command line option : $1"
      _usage
      ;;
  esac
  shift
done

# make sure jq exists
if [ "$(command -v jq)" = "" ]; then
  echo >&2 "jq command is required, see (https://stedolan.github.io/jq/download)";
  exit 1;
fi

_services

for service in ${services//,/ }
do
  "_""$service""_stats"
done

_system_stats
