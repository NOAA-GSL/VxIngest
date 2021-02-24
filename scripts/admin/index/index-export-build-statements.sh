#!/usr/bin/env bash

# ***********************************************************************************************************
# index-export-build-statements.sh
#
#   Usage: ./index-export-build-statement.sh [options]
#
#   This will gather all of the indexes build statements per bucket from a cluster.
#
#   Options:
#
#     --cluster=<s>           The cluster address (default: localhost)
#     --username=<s>          Cluster Admin or RBAC username (default: Administrator)
#     --password=<s>          Cluster Admin or RBAC password (default: password)
#     --output=<s>            Destination to output the file (default: indexes-build-2018-10-10T09:27:11.csv)
#     --port=<s>              The port to use (default: 8091)
#     --protocol=<s>          The protocol to use (default: http)
# ***********************************************************************************************************

# set the defaults, these can all be overriden as environment variables or passed via the cli
CB_USERNAME=${CB_USERNAME:='Administrator'}
CB_PASSWORD=${CB_PASSWORD:='password'}
CLUSTER=${CLUSTER:='localhost'}
PORT=${PORT:='8091'}
PROTOCOL=${PROTOCOL:='http'}
TIMEOUT=${TIMEOUT:=5}
OUTPUT_FILE="indexes-build-$(date +"%Y-%m-%dT%H:%M:%S").txt"

# parse any cli arguments
while [[ $# -gt 0 ]]; do
  case "$1" in
    -c|--cluster) CLUSTER=${2} && shift 2;;
    -r|--port) PORT=${2} && shift 2;;
    -s|--protocol) PROTOCOL=${2} && shift 2;;
    -t|--timeout) TIMEOUT=${2} && shift 2;;
    -f|--file) OUTPUT_FILE=${2} && shift 2;;
    -u|--username) CB_USERNAME=${2} && shift 2;;
    -p|--password)
      # if no password was specified prompt for one
      if [[ "${2:-}" == "" || "${2:-}" == --* ]]; then
        stty -echo # disable keyboard input
        read -p "Password: " -r CB_PASSWORD # prompt the user for the password
        stty echo # enable keyboard input
        echo # new line
        tput cuu1 && tput el # clear the previous line
        shift
      else
        CB_PASSWORD="${2}" # set the passed password
        shift 2
      fi
      ;;
    *)
      error "invalid option: '$1'."
      exit 1
      ;;
  esac
done

# make sure jq exists
if [ "$(command -v jq)" = "" ]; then
  echo >&2 "jq command is required, see (https://stedolan.github.io/jq/download)";
  exit 1;
fi

# get all of the indexes and their definitions
indexes_status=$(curl \
  --user "$CB_USERNAME:$CB_PASSWORD" \
  --silent \
  --connect-timeout "$TIMEOUT" \
  "$PROTOCOL://$CLUSTER:$PORT/indexStatus" | \
  jq -r '.indexes | sort_by(.bucket)')

# get all of the buckets that have indexes
buckets=$(echo "$indexes_status" | jq -r '[ .[] | .bucket ] | unique | sort | .[]')

# initialize the output file
echo "Output File: $OUTPUT_FILE"
echo
echo "exporting indexes per bucket..."
echo

# loop over each of the buckets
# shellcheck disable=SC2068
for bucket in ${buckets[@]}
do
  # get the indexes for the bucket and replace spaces w/ %20 (replicas are excluded)
  indexes=$(echo "$indexes_status" | jq -r "[ .[] |
    select(.bucket == \"$bucket\" and (.index | test(\"^[^ ]+$\"))) |
    (.index | sub(\" \"; \"%20\"; \"g\") ) | tostring | (  \"\`\" + . + \"\`\") ] | join(\",\")")

  echo "BUILD INDEX ON \`$bucket\`($indexes);" > "$OUTPUT_FILE"
done

echo
echo "export completed!"
echo
