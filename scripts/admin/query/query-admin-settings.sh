#!/usr/bin/env bash

# ***********************************************************************************************************
# query-settings.sh
#
#   Usage: ./query-settings.sh [options]
#
#   Gets the query settings for each query node in the cluster.  For more information the available settings
#   see: https://docs.couchbase.com/server/6.0/settings/query-settings.html
#
#   Options:
#
#     --cluster=<s>           The cluster address (default: localhost)
#     --username=<s>          Cluster Admin or RBAC username (default: Administrator)
#     --password=<s>          Cluster Admin or RBAC password (default: password)
#     --port=<s>              The port to use (default: 8091)
#     --queryPort=<s>         The query port to use (default: 8093)
#     --protocol=<s>          The protocol to use (default: http)
# ***********************************************************************************************************

# set the defaults, these can all be overriden as environment variables or passed via the cli
CB_USERNAME=${CB_USERNAME:='Administrator'}
CB_PASSWORD=${CB_PASSWORD:='password'}
CLUSTER=${CLUSTER:='localhost'}
PORT=${PORT:='8091'}
QUERY_PORT=${QUERY_PORT:='8093'}
PROTOCOL=${PROTOCOL:='http'}

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
    --queryPort=*)
      QUERY_PORT="${1#*=}"
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

# get all of the query nodes in the cluster and loop over them
# loop over each of the buckets
for node in $(curl \
  --user "$CB_USERNAME:$CB_PASSWORD" \
  --silent \
  "$PROTOCOL://$CLUSTER:$PORT/pools/default" | \
  jq -r '.nodes[] |
    select(.services | contains(["n1ql"]) == true) |
    .hostname | split(":")[0]'
  )
do
  echo "-------------------------------------------------------"
  echo "$node Query Admin Settings"
  echo "-------------------------------------------------------"

  # get the query node settings, output each of the properties
  curl \
    --user "$CB_USERNAME:$CB_PASSWORD" \
    --silent \
    --request GET \
    "$PROTOCOL://$node:$QUERY_PORT/admin/settings" | \
  jq -r '. | to_entries[] | "  " + .key + ": " + (.value | tostring)'
  echo "" # blank line
done
