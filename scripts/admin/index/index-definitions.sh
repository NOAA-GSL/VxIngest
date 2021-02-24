#!/usr/bin/env bash

# ***********************************************************************************************************
# index-definitions.sh
#
#   Usage: ./index-definitions.sh [options]
#
#   This will output the the definition for each index in the cluster
#
#   Options:
#
#     --cluster=<s>           The cluster address (default: localhost)
#     --username=<s>          Cluster Admin or RBAC username (default: Administrator)
#     --password=<s>          Cluster Admin or RBAC password (default: password)
#     --port=<s>              The port to use (default: 8091)
#     --protocol=<s>          The protocol to use (default: http)
# ***********************************************************************************************************

# set the defaults, these can all be overriden as environment variables or passed via the cli
CB_USERNAME=${CB_USERNAME:='Administrator'}
CB_PASSWORD=${CB_PASSWORD:='password'}
CLUSTER=${CLUSTER:='localhost'}
PORT=${PORT:='8091'}
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
  "$PROTOCOL://$CLUSTER:$PORT/indexStatus" | \
  jq -r '.indexes | sort_by(.bucket) | .[] | .definition' | \
  #Removing the nodes information so that the definitions can be used to create indexes in another cluster
  sed s/'"nodes":\[[^]]*],'// | \
  #Replacing the multiple spaces to single space so that the definitions can be used for comparisons
  sed -e 's/  */ /g' -e 's/^ *\(.*\) *$/\1/' | \
  #putting a ";" at the end so that the output so that can be used directly in the cbq utility
  sed -e 's/$/;/' | \
  #Removing the duplicate index definitions due to replicas
  uniq
  
