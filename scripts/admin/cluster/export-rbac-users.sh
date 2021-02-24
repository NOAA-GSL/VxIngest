#!/usr/bin/env bash

# ***********************************************************************************************************
# export-rbac-users.sh
#
#   Usage: ./export-rbac-users.sh [options]
#
#   Exports all of the RBAC users to a CSV file
#
#   Options:
#
#     --cluster=<s>           The cluster address (default: localhost)
#     --username=<s>          Cluster Admin or RBAC username (default: Administrator)
#     --password=<s>          Cluster Admin or RBAC password (default: password)
#     --port=<s>              The port to use (default: 8091)
#     --protocol=<s>          The protocol to use (default: http)
#     --output=<s>            Destination to output the file (default: rbac-users-2018-10-10T09:27:11.csv)
# ***********************************************************************************************************

# set the defaults, these can all be overriden as environment variables or passed via the cli
CB_USERNAME=${CB_USERNAME:='Administrator'}
CB_PASSWORD=${CB_PASSWORD:='password'}
CLUSTER=${CLUSTER:='localhost'}
PORT=${PORT:='8091'}
PROTOCOL=${PROTOCOL:='http'}
OUTPUT_FILE="rbac-users-$(date +"%Y-%m-%dT%H:%M:%S").csv"

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
    --output=*)
      OUTPUT_FILE="${1#*=}"
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

echo "Output File: $OUTPUT_FILE"

# get all of the rbac users (at most 100), if there is more than 100, at some point
# pagination will need to be implemented
curl \
  --user "$CB_USERNAME:$CB_PASSWORD" \
  --silent \
  --request GET \
  "$PROTOCOL://$CLUSTER:$PORT/settings/rbac/users?pageSize=100" | \
  # loop over all of the users, and ensure all of the properties exist and
  # transform the roles array into something that can be output as a csv
  jq -r '(reduce .users[] as $user ([]; . += [{
    id: $user.id,
    domain: $user.domain,
    name: ($user.name // ""),
    password_change_date: $user.password_change_date,
    roles: (reduce $user.roles[] as $roles ([]; . += [
      $roles.role + (if($roles.bucket_name != null) then "[" + $roles.bucket_name + "]" else "" end)
    ]))
  }])) as $users | [ $users[0] ] |
  (map(keys) | add | unique | sort) as $cols |
  $users | map(. as $row | $cols | map($row[.] | tostring)) as $rows |
  $cols,$rows[] | @csv' >> "$OUTPUT_FILE"
