#!/usr/bin/env bash

# ***********************************************************************************************************
# n1ql-performance-analysis.sh
#
#   Usage: ./n1ql-performance-analysis.sh [options]
#          ./n1ql-performance-analysis.sh -c CLUSTER -u Administrator -q 'select airportname from `travel-sample` where airportname' -n 15 -p
#
#   This performance analysis script will execute the given query for a specific number of times and prints the elapsed time on each execution
#   It will also calculate the minimum, maximum and average elapsed time as well
#
#   Options:
#
#     --cluster     The cluster address (default: localhost)
#     --username    Cluster Admin or RBAC username (default: Administrator)
#     --password    Cluster Admin or RBAC password (default: password)
#     --port        The port to use (default: 8091)
#     --protocol    The protocol to use (default: http)
#     --query-node  The query node to use (default: '')
#     --count	    No of times query has to execute(default: 10)
#     --n1ql        N1QL query that needs to be executed(default: SELECT * FROM system:keyspaces)
#     --timeout     Connection timeout duration(default: 5)
# ***********************************************************************************************************

# set the defaults, these can all be overriden as environment variables or passed via the cli
CLUSTER=${CLUSTER:='localhost'}
QUERY_NODE=${QUERY_NODE:=''}
USERNAME=${CB_USERNAME:='Administrator'}
PASSWORD=${CB_PASSWORD:='password'}
PORT=${PORT:='8091'}
PROTOCOL=${PROTOCOL:='http'}
TIMEOUT=${TIMEOUT:=5}
COUNT=${COUNT:=10}
QUERY=${QUERY:='SELECT * FROM system:keyspaces'}

# parse any cli arguments
while [[ $# -gt 0 ]]; do
  case "$1" in
    -c|--cluster) CLUSTER=${2} && shift 2;;
    -qn|--query-node) QUERY_NODE=${2} && shift 2;;
    -r|--port) PORT=${2} && shift 2;;
    -n|--count) COUNT=${2} && shift 2;;
    -q|--n1ql) QUERY=${2} && shift 2;;
    -s|--protocol) PROTOCOL=${2} && shift 2;;
    -t|--timeout) TIMEOUT=${2} && shift 2;;
    -u|--username) USERNAME=${2} && shift 2;;
    -p|--password)


      # if no password was specified prompt for one
      if [[ "${2:-}" == "" || "${2:-}" == --* ]]; then
        stty -echo # disable keyboard input
        read -p "Password: " -r PASSWORD # prompt the user for the password
        stty echo # enable keyboard input
        echo # new line
        tput cuu1 && tput el # clear the previous line
        shift
      else
        # shellcheck disable=SC2034
        PASSWORD="${2}" # set the passed password
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
# make sure bc exists
if [ "$(command -v bc)" = "" ]; then
  echo >&2 "bc command is required, see (https://www.gnu.org/software/bc/)";
  exit 1;
fi
#Custom Functions
# -----------------------------------
# Execute a N1QL statement
# -----------------------------------
# shellcheck disable=SC2001

executeN1ql() {
  local query_node="${1}"
  local statement="${2}"

  # call the nodes endpoint
  local url=$PROTOCOL://$query_node:8093/query/service

  local http_response
  http_response=$(curl \
    --user "$USERNAME:$PASSWORD" \
    --silent \
    --data-urlencode "statement=$statement" \
    --connect-timeout "$TIMEOUT" \
    --write-out "HTTPSTATUS:%{http_code}" \
    "$url")
  local http_body
  http_body=$(echo "$http_response" | sed -e 's/HTTPSTATUS\:.*//g')
  local http_status
  http_status=$(echo "$http_response" | tr -d '\n' | sed -e 's/.*HTTPSTATUS://')
  # inspect the response code
  if [ "$http_status" -eq "200" ]; then
    # parse the response, append the indexes from the cluster to the global indexes variable
    echo "$http_body" | jq --raw-output --compact-output \
      '.metrics'
  else
    echo "Unable to reach the query_node: ${query_node} at ${url}"
    exit 1
  fi
}

# getQueryNode
# -----------------------------------
# Gets all of the available query nodes in the cluster
# -----------------------------------
# shellcheck disable=SC2001
getQueryNodes() {
  local cluster="${1}"
  local query_nodes
  # call the nodes endpoint
  local url="$PROTOCOL://$CLUSTER:$PORT/pools/nodes"

  local http_response
  http_response=$(curl \
    --user "$USERNAME:$PASSWORD" \
    --silent \
    --connect-timeout "$TIMEOUT" \
    --request GET \
    --write-out "HTTPSTATUS:%{http_code}" \
    "$url")
  local http_body
  http_body=$(echo "$http_response" | sed -e 's/HTTPSTATUS\:.*//g')
  local http_status
  http_status=$(echo "$http_response" | tr -d '\n' | sed -e 's/.*HTTPSTATUS://')
  # inspect the response code
  if [ "$http_status" -eq "200" ]; then
    # parse the response, get the nodes with query services enabled
    query_nodes=$(echo "$http_body" | jq --raw-output --compact-output \
      --argjson input "$http_body" \
      '[
        .nodes[] |
        select(.services | contains(["n1ql"]) == true) |
        .hostname | split(":")[0]
        ] | join(",")
      ')
  else
    echo "Unable to reach the cluster: ${cluster} at ${url}"
    exit 1
  fi

  echo "$query_nodes"
}

# main script logic goes here

# if a query node was not passed (and it shouldn't be) get all of the available query
# nodes in the cluster
if [ "$QUERY_NODE" == "" ] || [ -z ${QUERY_NODE+x} ]; then
      query_nodes=$(getQueryNodes "$cluster")
else
      query_nodes="$QUERY_NODE"
fi
IFS=', ' read -r -a query_nodes <<< "$query_nodes"
query_node="${query_nodes[$((RANDOM % ${#query_nodes[@]}))]}"

min=0
max=0
avg=0
unit='ms'
time=0
sum=0
echo "Getting query execution elapsed time"
echo "------------------------------------"
for (( c=1; c<=COUNT; c++ ))
do
    out=$(executeN1ql "$query_node" "$QUERY")
    elapsedTime=$(echo "$out" | jq --raw-output --compact-output '.elapsedTime')
    recordCount=$(echo "$out" | jq --raw-output --compact-output '.resultCount')
    echo "$c => Time: $elapsedTime, Docs: $recordCount"

    time=${elapsedTime//[^0-9.]} #Removing all non-numeric characters
    if [ $c -eq 1 ];then
       min=$time
       max=$time
       sum=$time
       #unit=${out//[^a-zµ]}
       #Assume that the unit will be same for all executions
       unit=${elapsedTime//[0-9.]} #Removing all numeric characters
    else
       sum=$(echo "($sum + $time)" | bc -l)
    fi
    if (( $(echo "$time < $min" | bc -l) )); then
       min=$time
    fi
    if (( $(echo "$time > $max" | bc -l) )); then
       max=$time
    fi
done
echo "------------------------------------"
echo "MIN: $min$unit"
echo "MAX: $max$unit"
#calculating average and rounding off to 6 digits after decimal place
avg=$(echo "($sum / $COUNT)" | bc -l)
avg=$(printf "%.6f" "$avg")
echo "AVG: $avg$unit"

#Use of this source code is governed by Apache 2.0 license that can be found in the LICENSE file or at
#https://opensource.org/licenses/Apache-2.0.
