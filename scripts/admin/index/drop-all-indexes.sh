#!/usr/bin/env bash

# ***********************************************************************************************************
# drop-all-indexes.sh
#
#   Usage: ./drop-all-indexes.sh [options]
#
#   Drops all of the indexes from a cluster
#
#   Options:
#
#     --cluster=<s>           The cluster address (default: localhost)
#     --username=<s>          Cluster Admin or RBAC username (default: Administrator)
#     --password=<s>          Cluster Admin or RBAC password (default: password)
#     --port=<s>              The port to use (default: 8093)
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

# get all of the indexes for the cluster
indexes=$(curl \
  --user "$CB_USERNAME:$CB_PASSWORD" \
  --silent \
  "$PROTOCOL://$CLUSTER:$PORT/indexStatus" | \
  jq -r '[.indexes[] | "`" + .bucket + "`." + "`" + .index + "`"] | .[]')

index_count=$(echo "${indexes[@]}" | wc -w)

read -p "There are $index_count indexes in the $CLUSTER:$PORT cluster, are you sure you want to drop all of them (Y/N)? " -n 1 -r
echo

# if Y then proceed
if [[ $REPLY =~ ^[Yy]$ ]]
then
  # shellcheck disable=SC2068
  for index in ${indexes[@]}
  do
      # move the output back a line, erase it and output the current file being worked on
      # this is just to keep the shell output cleaner and it's cool ¯\_(ツ)_/¯
      output="Dropping Index: $index"
      echo "$output"

    # shellcheck disable=SC2000
    tput cuu1 && tput cuf "$(echo "$output" | wc -m)" \

      # call the query service api to delete the index
      curl \
        --user "$CB_USERNAME:$CB_PASSWORD" \
        --silent \
        --request POST \
        --data "statement=DROP INDEX $index" \
        "$PROTOCOL://$CLUSTER:8093/query/service" |
        jq -r '. |
          if(.status != "success") then
            "(" + .status + ": " + .errors[0].msg + ")"
          else
            "(" + .status + ")"
          end
        '
  done
fi
