#!/usr/bin/env bash

# ***********************************************************************************************************
# index-import-all-indexes.sh
#
#   Usage: ./index-import-all-indexes.sh [options]
#
#   This will import the the list of index definition in a given file into the cluster
#
#   Options:
#
#     --cluster=<s>           The cluster address (default: localhost)
#     --username=<s>          Cluster Admin or RBAC username (default: Administrator)
#     --password=<s>          Cluster Admin or RBAC password (default: password)
#     --port=<s>              The port to use (default: 8093)
#     --protocol=<s>          The protocol to use (default: http)
#     --file=<s>              File path to the list of index statement to load (default: index-statements.txt)
# ***********************************************************************************************************

# set the defaults, these can all be overriden as environment variables or passed via the cli
CB_USERNAME=${CB_USERNAME:='Administrator'}
CB_PASSWORD=${CB_PASSWORD:='password'}
CLUSTER=${CLUSTER:='localhost'}
PORT=${PORT:='8093'}
PROTOCOL=${PROTOCOL:='http'}
TIMEOUT=${TIMEOUT:=5}
FILE=${FILE:='index-statements.txt'}

# parse any cli arguments
while [[ $# -gt 0 ]]; do
  case "$1" in
    -c|--cluster) CLUSTER=${2} && shift 2;;
    -r|--port) PORT=${2} && shift 2;;
    -s|--protocol) PROTOCOL=${2} && shift 2;;
    -t|--timeout) TIMEOUT=${2} && shift 2;;
    -f|--file) FILE=${2} && shift 2;;
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

# function imports index create statements in the cluster
import_index_statement () {
   url_param="$( jq -nc --arg str "$index_statement;" '{"statement": $str}' )"
   url_param="statement=$index_statement;"
   echo "coding parameter: $url_param"
   curl \
    --request POST \
    --user "$CB_USERNAME:$CB_PASSWORD" \
    --data-urlencode "$url_param" \
    --silent \
    --connect-timeout "$TIMEOUT" \
    "$PROTOCOL://$CLUSTER:$PORT/query/service"
   echo "-- ------------------------------------------------------------ --"
}

# read file and add index
while IFS= read -r line
do
  index_statement="$line"
  import_index_statement
done < "$FILE"
