#!/usr/bin/env bash

# ***********************************************************************************************************
# eventing-service-stats-individual-node.sh
#
#   Usage: ./eventing-service-stats-for-node.sh [options]
#
#   This will output the eventing stats for the service on a specific node
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
  "$PROTOCOL://$CLUSTER:$PORT/pools/default/buckets/@eventing/nodes/$NODE/stats?zoom=$ZOOM" | \
  jq -r -c "def roundit: .*100.0 + 0.5|floor/100.0 | .*100.0;
  .op.samples |
  \"  bucket_op_exception_count: \" + (try (.[\"eventing/bucket_op_exception_count\"] | (. | add / length | roundit/100.0 | tostring)) catch \"N/A\") +
  \"\n  checkpoint_failure_count: \" + (try (.[\"eventing/checkpoint_failure_count\"] | (. | add / length | roundit/100.0 | tostring)) catch \"N/A\") +
  \"\n  dcp_backlog: \" + (try (.[\"eventing/dcp_backlog\"] | (. | add / length | roundit/100.0 | tostring)) catch \"N/A\") +
  \"\n  failed_count: \" + (try (.[\"eventing/failed_count\"] | (. | add / length | roundit/100.0 | tostring)) catch \"N/A\") +
  \"\n  n1ql_op_exception_count: \" + (try (.[\"eventing/n1ql_op_exception_count\"] | (. | add / length | roundit/100.0 | tostring)) catch \"N/A\") +
  \"\n  on_delete_failure: \" + (try (.[\"eventing/on_delete_failure\"] | (. | add / length | roundit/100.0 | tostring)) catch \"N/A\") +
  \"\n  on_delete_success: \" + (try (.[\"eventing/on_delete_success\"] | (. | add / length | roundit/100.0 | tostring)) catch \"N/A\") +
  \"\n  on_update_failure: \" + (try (.[\"eventing/on_update_failure\"] | (. | add / length | roundit/100.0 | tostring)) catch \"N/A\") +
  \"\n  on_update_success: \" + (try (.[\"eventing/on_update_success\"] | (. | add / length | roundit/100.0 | tostring)) catch \"N/A\") +
  \"\n  processed_count: \" + (try (.[\"eventing/processed_count\"] | (. | add / length | roundit/100.0 | tostring)) catch \"N/A\") +
  \"\n  timeout_count: \" + (try (.[\"eventing/timeout_count\"] | (. | add / length | roundit/100.0 | tostring)) catch \"N/A\")
  "
