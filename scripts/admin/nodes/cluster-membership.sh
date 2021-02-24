#!/usr/bin/env bash

# set the cluster username / password
CB_USERNAME='Administrator'
CB_PASSWORD='password'

# make sure jq exists
if [ "$(command -v jq)" = "" ]; then
  echo >&2 "jq command is required, see (https://stedolan.github.io/jq/download)";
  exit 1;
fi

curl \
  --user "$CB_USERNAME:$CB_PASSWORD" \
  --silent \
  http://localhost:8091/pools/nodes | \
  jq -r '.nodes[] | .hostname + " (" +.clusterMembership + ")"'
