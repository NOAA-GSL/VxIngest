#!/usr/bin/env bash

# ***********************************************************************************************************
# xdcr-bucket-stats.sh
#
#   Usage: ./xdcr-bucket-stats.sh [options]
#
#   This will output the xdcr stats for a specific buckets in the cluster, and explicitly
#   reference each of the available stats, and will correctly sum() or average()
#   the appropriate properties
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

# get the remote clusters
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

# get the xdcr stats for the bucket
curl \
  --user "$CB_USERNAME:$CB_PASSWORD" \
  --silent \
  "$PROTOCOL://$CLUSTER:$PORT/pools/default/buckets/@xdcr-$BUCKET/stats?zoom=$ZOOM" | \
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
    (. | split("/")[2]) as $destBucket |
    "Replication: " + . +
    ($remote_clusters | .[($replicationId | tostring)] |
      "\nRemote Cluster: " + .name +
      "\nHostname: " + .hostname
    ) +
    "\nDest Bucket: " + $destBucket +
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
    "\n  wtavg_meta_latency: " + (try ($stats["replications\/" + . + "\/wtavg_meta_latency"] | tostring) catch "N/A") +
    "\n"
  '
