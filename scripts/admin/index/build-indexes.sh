#!/usr/bin/env bash

# ***********************************************************************************************************
# build-indexes.sh
#
#   Usage: ./build-indexes.sh [options]
#
#   Build all of the indexes in a cluste
#
#   Options:
#
#     --cluster=<s>           The cluster address (default: localhost)
#     --username=<s>          Cluster Admin or RBAC username (default: Administrator)
#     --password=<s>          Cluster Admin or RBAC password (default: password)
#     --port=<s>              The port to use (default: 8091)
#     --protocol=<s>          The protocol to use (default: http)
#     --query-node=<s>        The query node to use (default: lookup first available n1ql node)
#     --query-port=<s>        The port to use (default: 8093)
# ***********************************************************************************************************

# set the defaults, these can all be overriden as environment variables or passed via the cli
CB_USERNAME=${CB_USERNAME:='Administrator'}
CB_PASSWORD=${CB_PASSWORD:='password'}
CLUSTER=${CLUSTER:='localhost'}
PORT=${PORT:='8091'}
PROTOCOL=${PROTOCOL:='http'}
QUERY_NODE=${QUERY_NODE:=''}
QUERY_PORT=${QUERY_PORT:='8093'}

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
    --query-node=*)
      QUERY_NODE="${1#*=}"
      ;;
    --query-port=*)
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

# get all of the indexes
unbuilt_indexes=$(curl \
  --user "$CB_USERNAME:$CB_PASSWORD" \
  --silent \
  "$PROTOCOL://$CLUSTER:$PORT/indexStatus" |
  jq -r '[ .indexes[] | select((.index | contains("replica") | not) and .status == "Created") | { index:.index, bucket: .bucket } ] | sort_by(.bucket)')

# find a query node in the cluster to use if it was not specified
if [[ "$QUERY_NODE" == "" ]]
then
  QUERY_NODE=$(curl \
    --user "$CB_USERNAME:$CB_PASSWORD" \
    --silent \
    "$PROTOCOL://$CLUSTER:$PORT/pools/nodes" | \
    jq -r '[
      .nodes[] |
      select(.services | contains(["n1ql"]) == true) |
      .hostname | split(":")[0]
      ][0]
    '
    )
fi

# loop over each of the indexes
for bucket in $(echo "$unbuilt_indexes" | jq -r '[ .[].bucket ] | unique | sort | .[]'); do
  output="Building Indexes on Bucket: $bucket"
  echo -en "\r\033[K$output"
  bucket_indexes=$(echo "$unbuilt_indexes" | jq -r --arg bucket "$bucket" '[.[] | select(.bucket == $bucket) | "`" + .index + "`"] | join(",")')
  # build the index
  N1QL="BUILD INDEX ON \`$bucket\` ($bucket_indexes)"

  # call the query service api start the build of the indexes
  curl \
    --user "$CB_USERNAME:$CB_PASSWORD" \
    --silent \
    --request POST \
    --data "statement=$N1QL" \
    "$PROTOCOL://$QUERY_NODE:$QUERY_PORT/query/service" |
    jq -r '. |
      if(.status != "success") then
        " (" + .status + ": " + .errors[0].msg + ")"
      else
        " (" + .status + ")"
      end
    '
done
echo -en "\r\033[K"
echo ""
