#!/usr/bin/env bash

# ***********************************************************************************************************
# drop-index.sh
#
#   Usage: ./drop-index.sh [options]
#
#   Drops all of the indexes from a specific bucket
#
#   Options:
#
#     --cluster=<s>           The cluster address (default: localhost)
#     --username=<s>          Cluster Admin or RBAC username (default: Administrator)
#     --password=<s>          Cluster Admin or RBAC password (default: password)
#     --bucket=<s>            The name of the bucket (default: default)
#     --index=<s>             The name of the index to drop (default: "")
#     --port=<s>              The port to use (default: 8093)
#     --protocol=<s>          The protocol to use (default: http)
# ***********************************************************************************************************

# set the defaults, these can all be overriden as environment variables or passed via the cli
CB_USERNAME=${CB_USERNAME:='Administrator'}
CB_PASSWORD=${CB_PASSWORD:='password'}
CLUSTER=${CLUSTER:='localhost'}
BUCKET=${BUCKET:='default'}
INDEX=${INDEX:=''}
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
    --bucket=*)
      BUCKET="${1#*=}"
      ;;
    --index=*)
      INDEX="${1#*=}"
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

read -p "Are you sure you want to drop the \`$BUCKET\`.\`$INDEX\` (Y/N)? " -n 1 -r
echo

# if Y then proceed
if [[ $REPLY =~ ^[Yy]$ ]]
then
    # output each index and the result on it's own line
    output="Dropping Index: \`$BUCKET\`.\`$INDEX\`"
    echo "$output"
    # shellcheck disable=SC2000
    tput cuu1 && tput cuf "$(echo "$output" | wc -m)" \

    # call the query service api to delete the index
    curl \
      --user "$CB_USERNAME:$CB_PASSWORD" \
      --silent \
      --request POST \
      --data "statement=DROP INDEX \`$BUCKET\`.\`$INDEX\`" \
      "$PROTOCOL://$CLUSTER:8093/query/service" |
      jq -r '. |
        if(.status != "success") then
          "(" + .status + ": " + .errors[0].msg + ")"
        else
          "(" + .status + ")"
        end
      '
fi
