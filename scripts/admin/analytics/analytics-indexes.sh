#!/usr/bin/env bash

# ***********************************************************************************************************
# analytics-indexes.sh
#
#   Usage: ./analytics-indexes.sh [options]
#
#   This will output all of the available analytics indexes in the cluster
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
PORT=${PORT:='8095'}
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

n1ql="
SELECT DataverseName, DatasetName, IndexName,
    (
        SELECT VALUE STRING_JOIN(keyfield, '.') || ':' || idx.SearchKeyType[pos-1]
        FROM idx.SearchKey AS keyfield AT pos
        ORDER BY pos
    ) AS Keys
FROM \`Metadata\`.\`Index\`
WHERE DataverseName <> 'Metadata'
    AND IsPrimary = false
ORDER BY DataverseName, DatasetName, IsPrimary DESC;
"

curl \
  --user "$CB_USERNAME:$CB_PASSWORD" \
  --silent \
  --data-urlencode "statement=$n1ql" \
  "$PROTOCOL://$CLUSTER:$PORT/analytics/service" | \
  jq -r '.results[] |
    "Dataverse: " + .DataverseName + ", Dataset: " + .DatasetName + ", Index:" + .IndexName + ", Keys: " + (.Keys | tostring)'
