#!/usr/bin/env bash

# ***********************************************************************************************************
# build-bucket-indexes.sh
#
#   Usage: ./build-bucket-indexes.sh [options]
#
#   Build all of the indexes for a bucket which are not yet built
#
#   Options:
#
#     --cluster=<s>           The cluster address (default: localhost)
#     --username=<s>          Cluster Admin or RBAC username (default: Administrator)
#     --password=<s>          Cluster Admin or RBAC password (default: password)
#     --port=<s>              The port to use (default: 8091)
#     --protocol=<s>          The protocol to use (default: http)
#     --bucket=<s>            The name of the bucket (default: default)
#     --query-node=<s>        The query node to use (default: lookup first available n1ql node)
#     --query-port=<s>        The port to use (default: 8093)
# ***********************************************************************************************************

# set the defaults, these can all be overriden as environment variables or passed via the cli
CB_USERNAME=${CB_USERNAME:='Administrator'}
CB_PASSWORD=${CB_PASSWORD:='password'}
CLUSTER=${CLUSTER:='localhost'}
PORT=${PORT:='8091'}
PROTOCOL=${PROTOCOL:='http'}
BUCKET=${BUCKET:='default'}
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
    --bucket=*)
      BUCKET="${1#*=}"
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

# get all of the indexes for the bucket which are not yet built
indexes=$(curl \
  --user "$CB_USERNAME:$CB_PASSWORD" \
  --silent \
  "$PROTOCOL://$CLUSTER:$PORT/indexStatus" | \
  jq -r --arg bucket "$BUCKET" '[.indexes[] | select(.bucket == $bucket and (.index | contains("replica") | not) and .status == "Created") | "`" + .index + "`"] | join(",")')

if [ -z "$indexes" ]
then
  echo "There are no Indexes to be build in $BUCKET"
  exit
fi

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

N1QL="BUILD INDEX ON \`$BUCKET\` ($indexes)"

# call the query service api start the build of the indexes
curl \
  --user "$CB_USERNAME:$CB_PASSWORD" \
  --silent \
  --request POST \
  --data "statement=$N1QL" \
  "$PROTOCOL://$QUERY_NODE:$QUERY_PORT/query/service" |
  jq -r '. |
    if(.status != "success") then
      "(" + .status + ": " + .errors[0].msg + ")"
    else
      "(" + .status + ")"
    end
  '
