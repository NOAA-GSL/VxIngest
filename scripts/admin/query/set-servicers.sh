#!/usr/bin/env bash

# ***********************************************************************************************************
# set-servicers.sh
#
#   Usage: ./set-servicers.sh [options]
#
#   Sets the Query Service Completed SERVICERS on all Query Nodes in the Cluster
#
#   Options:
#
#     --cluster=<s>           The cluster address (default: localhost)
#     --username=<s>          Cluster Admin or RBAC username (default: Administrator)
#     --password=<s>          Cluster Admin or RBAC password (default: password)
#     --port=<s>              The port to use (default: 8091)
#     --queryPort=<s>         The query port to use (default: 8093)
#     --protocol=<s>          The protocol to use (default: http)
#     --servicers=<n>         The number of servicers to use
# ***********************************************************************************************************

# set the defaults, these can all be overriden as environment variables or passed via the cli
CB_USERNAME=${CB_USERNAME:='Administrator'}
CB_PASSWORD=${CB_PASSWORD:='password'}
CLUSTER=${CLUSTER:='localhost'}
PORT=${PORT:='8091'}
QUERY_PORT=${QUERY_PORT:='8093'}
PROTOCOL=${PROTOCOL:='http'}
SERVICERS=${SERVICERS:='64'}

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
    --servicers=*)
      SERVICERS="${1#*=}"
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
  # save the output string so we can move back a line, then output the message
  output="$node - "
  echo "$output"
  # move back up 1 line and move to the end of the line
  # shellcheck disable=SC2000
  tput cuu1 && tput cuf "$(echo "$output" | wc -m)"

  # call the query admin api
  response=$(curl \
    --user "$CB_USERNAME:$CB_PASSWORD" \
    --silent \
    --request POST \
    --data "{\"servicers\":$SERVICERS}" \
    --write-out "%{http_code}" \
    --output /dev/null \
    "$PROTOCOL://$node:$QUERY_PORT/admin/settings")

  # inspect the response code
  if [ "$response" -eq "200" ]; then
     echo "(success)"
  else
     echo "(failed)"
  fi
done
