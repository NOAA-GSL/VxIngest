#!/usr/bin/env bash

# ***********************************************************************************************************
# per-node-mapreduce-stats.sh
#
#   Usage: ./per-node-mapreduce-stats.sh [options]
#
#   This will output the map reduce stats per node for each design document for all bucket
#   in the cluster, and explicitly reference each of the available stats, and will correctly sum() or average()
#   the appropriate properties
#
#   Options:
#
#     --cluster=<s>           The cluster address (default: localhost)
#     --username=<s>          Cluster Admin or RBAC username (default: Administrator)
#     --password=<s>          Cluster Admin or RBAC password (default: password)
#     --port=<s>              The port to use (default: 8091)
#     --protocol=<s>          The protocol to use (default: http)
#     --zoom=<s>              The option to sample minute, hour, day, week, year (default: minute)
# ***********************************************************************************************************

# set the defaults, these can all be overriden as environment variables or passed via the cli
CB_USERNAME=${CB_USERNAME:='Administrator'}
CB_PASSWORD=${CB_PASSWORD:='password'}
CLUSTER=${CLUSTER:='localhost'}
PORT=${PORT:='8091'}
PROTOCOL=${PROTOCOL:='http'}
ZOOM=${ZOOM:='minute'}

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
    --zoom=*)
      ZOOM="${1#*=}"
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

# get the data nodes in the cluster
nodes=$(curl \
  --user "$CB_USERNAME:$CB_PASSWORD" \
  --silent \
  --request GET \
  "$PROTOCOL://$CLUSTER:$PORT/pools/nodes" | \
  jq -r '.nodes[] | select(.services[] | contains("kv")) | .hostname')

# loop over each of the buckets
for bucket in $(curl \
  --user "$CB_USERNAME:$CB_PASSWORD" \
  --silent \
  --request GET \
  --data skipMap=true \
  "$PROTOCOL://$CLUSTER:$PORT/pools/default/buckets" | \
  jq -r '.[].name')
do
  # call the stats directory to get each of the ddoc Ids
  ddocs=$(curl \
    --user "$CB_USERNAME:$CB_PASSWORD" \
    --silent \
    "$PROTOCOL://$CLUSTER:$PORT/pools/default/buckets/$bucket/statsDirectory" | \
    jq -r -c '[
    .blocks[] | select(.blockName | contains("Mapreduce")) |
    reduce(.) as $ddoc (
      {};
      . = {
        name: ($ddoc.blockName | gsub("Mapreduce View Stats: "; "") | gsub("_design/dev[A-Za-z0-9_]+ "; "")),
        stats: [$ddoc.stats[] | .name ],
        ddocId: ($ddoc.stats[0].name | gsub("views/"; "") | gsub("/.+"; ""))
      }
    ) ]')
  # make sure that there are design documents for the bucket, otherwise skip it
  if [[ $ddocs != "[]" ]]; then
    echo "Bucket: $bucket"
    echo "================================================================"
    # loop over each node in the cluster
    # shellcheck disable=SC2068
    for node in ${nodes[@]}
    do
      # get the bucket stats for the node
      curl \
        --user "$CB_USERNAME:$CB_PASSWORD" \
        --silent \
        --request GET \
        "$PROTOCOL://$CLUSTER:$PORT/pools/default/buckets/$bucket/nodes/$node/stats?zoom=$ZOOM" | \
        jq -r --arg node "$node" --argjson ddocs "$ddocs" '
        reduce (.op.samples | to_entries[]) as {$key, $value} (
          {};
          if (($key | startswith("views/"))) then
            if (($key | contains("accesses"))) then
              .[$key] += ($value | add)
            else
              .[$key] += ($value | add / length)
            end
          else
            .
          end
        ) | . as $stats |
        $ddocs[] | .name + " (" + $node + ")" +
        "\n----------------------------------------------------------------" +
        "\n" + (
          reduce(.stats[]) as $statName (
            "";
            . += ($statName | split("/")[2]) + ": " + ($stats[$statName] | tostring) + "\n"
          )
        )
        '
    done
  fi
done
