#!/usr/bin/env bash

# ***********************************************************************************************************
# getcbversion.sh
#
#   Usage: ./getcbversion.sh -c CLUSTER1,CLUSTER2 -u Administrator -p
#
#   This script will list down the couchbase versions installed in each node of the clusters.
#
#   Options:
#
#     --cluster     Multiple cluster addresses in csv format(default: localhost)
#     --username    Cluster Admin or RBAC username (default: Administrator)
#     --password    Cluster Admin or RBAC password (default: password)
#     --port        The port to use (default: 8091)
#     --protocol    he protocol to use (default: http)
#     --timeout     Connection timeout duration(default: 5)
# ***********************************************************************************************************

# set the defaults, these can all be overriden as environment variables or passed via the cli
CLUSTER=${CLUSTER:='localhost'}
USERNAME=${CB_USERNAME:='Administrator'}
PASSWORD=${CB_PASSWORD:='password'}
PORT=${PORT:='8091'}
PROTOCOL=${PROTOCOL:='http'}
TIMEOUT=${TIMEOUT:=5}

# parse any cli arguments
while [[ $# -gt 0 ]]; do
  case "$1" in
    -c|--cluster) CLUSTER=${2} && shift 2;;
    -r|--port) PORT=${2} && shift 2;;
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
      #error "invalid option: '$1'."
      exit 1
      ;;
  esac
done

# make sure jq exists
if [ "$(command -v jq)" = "" ]; then
  echo >&2 "jq command is required, see (https://stedolan.github.io/jq/download)";
  exit 1;
fi


# getCBClusterVersion
# -----------------------------------
# Gets all of the available query nodes in the cluster
# -----------------------------------
# shellcheck disable=SC2001
getCBClusterVersion() {
  local cluster="${1}"
  local cluster_name
  local cb_versions
  # call the nodes endpoint
  local url="$PROTOCOL://$cluster:$PORT/pools/nodes"

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
    # parse the response, append the indexes from the cluster to the global indexes variable

    cluster_name=$(echo "$http_body" | jq '.clusterName')
    cb_versions=$(echo "$http_body" | jq -r '.nodes | map((.hostname | split(":")[0]) + ": " + .version)')

  else
    echo "Unable to reach the cluster: ${cluster} at ${url}"
    exit 1
  fi

  echo -e "$cluster_name: $cb_versions"
}

# main script logic goes here
clusters=$(echo "$CLUSTER" | tr "," "\n")

for c in $clusters
do
  versions="$(getCBClusterVersion "$c")"
  echo -e "$versions"
done

#Use of this source code is governed by Apache 2.0 license that can be found in the LICENSE file or at
#https://opensource.org/licenses/Apache-2.0.
