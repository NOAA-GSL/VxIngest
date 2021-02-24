#!/usr/bin/env bash

version="1.2.0"

_usage() {
  echo -n "${__script} [OPTION]...

 This will output slow queries, the total # and the average duration of the query

 Options:
  -c, --cluster(s)        A comma-delimited list of one or more clusters to retrieve the slow queries from.  (default: localhost)
  -u, --username          Cluster Admin or RBAC username (default: Administrator)
  -p, --password          Cluster Admin or RBAC password (default: password)
  -s, --sort              (optional) The order in which to sort the results.  Values can be:
                          (time, queries, size, results, fetches, scan, selectivity).
                          *Note: The values are always first sorted by node name, then the value specified here.
  -i, --include-indexes   (optional) Whether or not to include the index(es) that the queries explain
                          resolves to.  (default: false)
                          *Note: When setting this to true, each identified slow query must be EXPLAINed and the
                          report will take longer to generate because of this.
  -a, --include-clients   (optional) Whether or not to include the unique clientContextID for each query that has
                          executed the query.  (default: false)
  -r, --port              (optional) The port to use (default: 8091)
  -l, --protocol          (optional) The protocol to use (default: http)
  -t, --timeout           (optional) The timeout to use for HTTP requests (default: 5)
  -n, --query-node        (optional) A comma-delimited list of query nodes to use. If not specified all are used. (default: none)
                          This should only be used on a local machine, if Couchbase is being ran inside of docker,
                          and only a single cluster is being used.
  -q, --query-port        (optional) The query port to use (default: 8093)
  -d, --start-date        (optional) The date to start returning slow queries from (default: none)
  -e, --end-date          (optional) The end date to stop returning slow queries from (default: none)
  -o, --output-dir        (optional) The name of the output directory to use if output is csv (default: pwd)
  -f, --output-file       (optional) The name of the output file if output is csv (default: index-usage-yyyy-mm-ddThh:mm:ss)
  -m, --max-chars         (optional) The maximum number of characters to return for the statement (default: 200)
  --log-level             The log level to to use 0-7 (default: 6)
  --debug                 Shortcut for --log-level 7
  --help                  Display this help and exit
  --version               Output version information and exit
"
}

# default variables / flags and their optional corresponding environment variables used in the script
CLUSTERS=${CLUSTERS:='localhost'}
USERNAME=${CB_USERNAME:='Administrator'}
PASSWORD=${CB_PASSWORD:='password'}
PORT=${PORT:='8091'}
PROTOCOL=${PROTOCOL:='http'}
TIMEOUT=${TIMEOUT:=5}
SORT=${SORT:='time'}
INCLUDE_INDEXES=${INCLUDE_INDEXES:=false}
INCLUDE_CLIENTS=${INCLUDE_CLIENTS:=false}
QUERY_NODE=${QUERY_NODE:=''}
QUERY_PORT=${QUERY_PORT:='8093'}
OUTPUT_DIR=${OUTPUT_DIR:=$(pwd)}
OUTPUT_FILE="queries-$(date +"%Y-%m-%dT%H:%M:%S").csv"
START_DATE=${START_DATE:='none'}
END_DATE=${END_DATE:='none'}
MAX_CHARS=${MAX_CHARS:='200'}

# _options
# -----------------------------------
# Parses CLI options
# -----------------------------------
_options() {
  debug ""
  while [[ $# -gt 0 ]]; do
    case "$1" in
      -c|--cluster|--clusters) CLUSTERS=${2} && shift 2;;
      -r|--port) PORT=${2} && shift 2;;
      -l|--protocol) PROTOCOL=${2} && shift 2;;
      -t|--timeout) TIMEOUT=${2} && shift 2;;
      -s|--sort) SORT=${2} && shift 2;;
      -i|--include-indexes) INCLUDE_INDEXES=${2} && shift 2;;
      -a|--include-clients) INCLUDE_CLIENTS=${2} && shift 2;;
      -n|--query-node) QUERY_NODE=${2} && shift 2;;
      -q|--query-port) QUERY_PORT=${2} && shift 2;;
      -d|--start-date) START_DATE=${2} && shift 2;;
      -e|--end-date) END_DATE=${2} && shift 2;;
      -u|--username) USERNAME=${2} && shift 2;;
      -o|--output-dir) OUTPUT_DIR=${2} && shift 2;;
      -f|--output-file) OUTPUT_FILE=${2} && shift 2;;
      -m|--max-chars) MAX_CHARS=${2} && shift 2;;
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
}

# _dependencies
# -----------------------------------
# Ensure script dependencies exist
# -----------------------------------
_dependencies() {
  debug ""
  # check if jq is installed
  if [ "$(command -v jq)" = "" ]; then
    emergency "jq command is required, see (https://stedolan.github.io/jq/download)"
  fi
}

# validate
# -----------------------------------
# Validate Params
# -----------------------------------
_validate() {
  debug ""
  local valid=true
  # validate the cluster argument does not contain any port references
  if [[ "$CLUSTERS" =~ :[0-9]+ ]]; then
    warning "Do not specifiy the port for the hostnames in the -c/--clusters argument" && valid=false
  fi
  # validate the cluster argument does not contain the protocol
  if [[ "$CLUSTERS" =~ https?:// ]]; then
    warning "Do not specifiy the protocol (http/https) for the hostnames in the -c/--clusters argument" && valid=false
  fi
  # validate that there is a username
  if [[ -z "$USERNAME" ]]; then
    warning "The -u/--username argument is required" && valid=false
  fi
  # validate that there is a password
  if [[ -z "$PASSWORD" ]]; then
    warning "The -p/--password argument is required" && valid=false
  fi
  # validate the protocol argument is http/https
  if ! [[ "$PROTOCOL" =~ ^https?$ ]]; then
    warning "The -s/--protocol argument can only be \"http\" or \"https\"" && valid=false
  fi
  # validate the port argument is a number
  if ! [[ "$PORT" =~ ^[1-9][0-9]*$ ]]; then
    warning "The -r/--port argument must be an integer greater than 0" && valid=false
  fi
  # validate the timeout argument is a number
  if ! [[ "$TIMEOUT" =~ ^[1-9][0-9]*$ ]]; then
    warning "The -t/--timeout argument must be an integer greater than 0" && valid=false
  fi
  # validate the log level is between 0-7 argument is a number
  if ! [[ "$LOG_LEVEL" =~ ^[0-7]$ ]]; then
    warning "The -l/--log-level argument must be an integer between 0-7" && valid=false
  fi
  # validate the query-node argument does not contain any port references
  if [[ "$QUERY_NODE" =~ :[0-9]+ ]]; then
    warning "Do not specifiy the port for the hostnames in the -n/--query-node argument" && valid=false
  fi
  # validate the query-node argument does not contain the protocol
  if [[ "$QUERY_NODE" =~ https?:// ]]; then
    warning "Do not specifiy the protocol (http/https) for the hostnames in the -n/--query-node argument" && valid=false
  fi
  # validate the port argument is a number
  if ! [[ "$QUERY_PORT" =~ ^[1-9][0-9]*$ ]]; then
    warning "The -q/--query-port argument must be an integer greater than 0" && valid=false
  fi
  # validate the include indexes argument is a boolean
  if ! [[ "$INCLUDE_INDEXES" =~ ^(true|false)$ ]]; then
    warning "The -i/--include-indexes argument can only be \"true\" or \"false\"" && valid=false
  fi
  # validate the include clients argument is a boolean
  if ! [[ "$INCLUDE_CLIENTS" =~ ^(true|false)$ ]]; then
    warning "The -a/--include-clients argument can only be \"true\" or \"false\"" && valid=false
  fi
  # validate max chars is a number
   if ! [[ "$MAX_CHARS" =~ ^[0-9]+$ ]]; then
     warning "The -m/--max-chars argument must be an integer" && valid=false
   fi
  # if there are errors
  if ( ! $valid ); then
    exit 1
  fi
}

# main
# -----------------------------------
# Main function
# -----------------------------------
main() {
  # log the invocation command and arguments
  debug "
  invocation:
    $__invocation
  arguments:
    clusters: $CLUSTERS
    username: $USERNAME
    password: ********
    sort: $SORT
    include_indexes: $INCLUDE_INDEXES
    include_clients: $INCLUDE_CLIENTS
    start_date: $START_DATE
    end_date: $END_DATE
    port: $PORT
    protocol: $PROTOCOL
    timeout: $TIMEOUT
    query_port: $QUERY_PORT
    output_dir: $OUTPUT_DIR
    output_file: $OUTPUT_FILE"

  # set the headings to be used in the report
  local headings
  headings=$(getColumnHeadings "$INCLUDE_INDEXES" "$INCLUDE_CLIENTS")

  # write out the initial headings
  echo "$headings" > "$OUTPUT_DIR/$OUTPUT_FILE"

  # build the slow query n1ql statement to be used
  local n1ql
  n1ql=$(getSlowQueryStatement "$INCLUDE_CLIENTS" "$START_DATE" "$END_DATE" "$SORT")

  # loop over each of the clusters and get all of the indexes
  for cluster in $(echo "$CLUSTERS" | jq --slurp --raw-output --raw-input 'split(",") | .[]')
  do
    # local variable to hold the name of the cluster
    local cluster_name
    cluster_name=$(getClusterName "$cluster")
    echo -en "\r\033[KCluster: $cluster"

    # local variable to hold the query node to be used
    local query_nodes
    # if a query node was not passed (and it shouldn't be) get all of the available query
    # nodes in the cluster
    if [ "$QUERY_NODE" == "" ] || [ -z ${QUERY_NODE+x} ]; then
      query_nodes=$(getQueryNodes "$cluster")
    else
      query_nodes="$QUERY_NODE"
    fi
    debug "query_nodes: $query_nodes"
    # convert the query nodes list into an array, so we can loop over each query node
    # and get just the slow queries that are present on that node.  This will prevent
    # performing the default scatter gather when querying system:completed_requests.
    IFS=', ' read -r -a query_nodes <<< "$query_nodes"

    # loop over each of the query nodes in the cluster
    for node in "${query_nodes[@]}"
    do
      echo -en "\r\033[KCluster: $cluster  ||  Query Node: $node"
      local results
      # execute the slow query n1ql statement against the current query node, pass the n1ql statement by stripping line breaks
      results=$(executeN1ql "$node" "${n1ql//$'\n'/}")

      # if we're not include in the indexes or clientContextIDs
      if [[ $INCLUDE_CLIENTS == false ]] && [[ $INCLUDE_INDEXES == false ]]; then
        echo "$results" | jq  --raw-output --arg cluster_name "$cluster_name" --arg node "$node" '.[] | [
          $cluster_name,
          $node,
          (.statement | gsub("[\\n\\t\\r]"; "")),
          .firstSeenAt,
          .lastExecutedAt,
          .querySelectivity,
          .avgFetches,
          .minFetches,
          .maxFetches,
          .avgResults,
          .minResults,
          .maxResults,
          .avgScanResults,
          .minScanResults,
          .maxScanResults,
          .avgServiceTime,
          .minServiceTime,
          .maxServiceTime,
          .avgServiceTime_ms,
          .minServiceTime_ms,
          .maxServiceTime_ms,
          .avgElapsedTime,
          .minElapsedTime,
          .maxElapsedTime,
          .avgElapsedTime_ms,
          .minElapsedTime_ms,
          .maxElapsedTime_ms,
          .avgQueueTime,
          .minQueueTime,
          .maxQueueTime,
          .avgQueueTime_us,
          .minQueueTime_us,
          .maxQueueTime_us,
          .avgSizeBytes,
          .minSizeBytes,
          .maxSizeBytes,
          .totalQueries
        ] | @csv' >> "$OUTPUT_DIR/$OUTPUT_FILE"
      # if we're including just the clientContextIDs but not the indexes
      elif [[ $INCLUDE_CLIENTS == true ]] && [[ $INCLUDE_INDEXES == false ]]; then
        echo "$results" | jq  --raw-output --arg cluster_name "$cluster_name" --arg node "$node" '.[] | [
          $cluster_name,
          $node,
          (.statement | gsub("[\\n\\t\\r]"; "")),
          .firstSeenAt,
          .lastExecutedAt,
          .querySelectivity,
          .avgFetches,
          .minFetches,
          .maxFetches,
          .avgResults,
          .minResults,
          .maxResults,
          .avgScanResults,
          .minScanResults,
          .maxScanResults,
          .avgServiceTime,
          .minServiceTime,
          .maxServiceTime,
          .avgServiceTime_ms,
          .minServiceTime_ms,
          .maxServiceTime_ms,
          .avgElapsedTime,
          .minElapsedTime,
          .maxElapsedTime,
          .avgElapsedTime_ms,
          .minElapsedTime_ms,
          .maxElapsedTime_ms,
          .avgQueueTime,
          .minQueueTime,
          .maxQueueTime,
          .avgQueueTime_us,
          .minQueueTime_us,
          .maxQueueTime_us,
          .avgSizeBytes,
          .minSizeBytes,
          .maxSizeBytes,
          .totalQueries,
          .clientContextIDs
        ] | @csv' >> "$OUTPUT_DIR/$OUTPUT_FILE"
      # we're including the indexes that are used and it gets a little more complicated as an
      # EXPLAIN needs to be issued for each query found
      elif [[ $INCLUDE_INDEXES == true ]]; then
        local results_cnt
        results_cnt=$(echo "$results" | jq --raw-output '. | length')
        local counter=0
        # include indexes is true, so we need to loop over each of the identified queries
        # and issue an explain on it to pull out the indexes that are used
        for row in $(echo "$results" | jq --raw-output '.[] | @base64'); do
          # increment the counter and output the current query
          counter=$((counter+1))
          echo -en "\r\033[KCluster: $cluster  ||  Query Node: $node  ||  Query: $counter/$results_cnt"

          _jq() {
           echo "$row" | base64 --decode | jq -r "${1}"
          }
          # parse the individual row
          local result
          result=$(_jq '.')
          # get the query statement that is used
          local query
          query=$(echo "$result" | jq --raw-output '.statement')
          # execute an explain for the query
          local explain
          explain=$(executeN1ql "${query_nodes[$((RANDOM % ${#query_nodes[@]}))]}" "EXPLAIN $query") 2>&1 || echo ""
          # pull any "index":"*" statements from the explain
          local indexes
          indexes=$(echo "$explain" | (grep -E -o  '"index":"[^"]+"' 2>&1) | (tr '\n' ', ' 2>&1 || echo "") | (sed -E 's/"index":"//g' 2>&1 || echo "") | (sed -E 's/",/, /g' 2>&1 || echo "") | (sed -E 's/, $//' 2>&1 || echo "")) || echo ""
          # output to csv file
          if [[ $INCLUDE_CLIENTS == true ]]; then
            echo "$result" | jq --raw-output --arg cluster_name "$cluster_name" --arg node "$node" --arg indexes "$indexes" '
              . += { "indexes": $indexes } |
              [
                $cluster_name,
                $node,
                (.statement | gsub("[\\n\\t\\r]"; "")),
                .firstSeenAt,
                .lastExecutedAt,
                .querySelectivity,
                .avgFetches,
                .minFetches,
                .maxFetches,
                .avgResults,
                .minResults,
                .maxResults,
                .avgScanResults,
                .minScanResults,
                .maxScanResults,
                .avgServiceTime,
                .minServiceTime,
                .maxServiceTime,
                .avgServiceTime_ms,
                .minServiceTime_ms,
                .maxServiceTime_ms,
                .avgElapsedTime,
                .minElapsedTime,
                .maxElapsedTime,
                .avgElapsedTime_ms,
                .minElapsedTime_ms,
                .maxElapsedTime_ms,
                .avgQueueTime,
                .minQueueTime,
                .maxQueueTime,
                .avgQueueTime_us,
                .minQueueTime_us,
                .maxQueueTime_us,
                .avgSizeBytes,
                .minSizeBytes,
                .maxSizeBytes,
                .totalQueries,
                .indexes,
                .clientContextIDs
              ] | @csv' >> "$OUTPUT_DIR/$OUTPUT_FILE"
          else
            echo "$result" | jq --raw-output --arg cluster_name "$cluster_name" --arg node "$node" --arg indexes "$indexes" '
              . += { "indexes": $indexes } |
              [
                $cluster_name,
                $node,
                (.statement | gsub("[\\n\\t\\r]"; "")),
                .firstSeenAt,
                .lastExecutedAt,
                .querySelectivity,
                .avgFetches,
                .minFetches,
                .maxFetches,
                .avgResults,
                .minResults,
                .maxResults,
                .avgScanResults,
                .minScanResults,
                .maxScanResults,
                .avgServiceTime,
                .minServiceTime,
                .maxServiceTime,
                .avgServiceTime_ms,
                .minServiceTime_ms,
                .maxServiceTime_ms,
                .avgElapsedTime,
                .minElapsedTime,
                .maxElapsedTime,
                .avgElapsedTime_ms,
                .minElapsedTime_ms,
                .maxElapsedTime_ms,
                .avgQueueTime,
                .minQueueTime,
                .maxQueueTime,
                .avgQueueTime_us,
                .minQueueTime_us,
                .maxQueueTime_us,
                .avgSizeBytes,
                .minSizeBytes,
                .maxSizeBytes,
                .totalQueries,
                .indexes
              ] | @csv' >> "$OUTPUT_DIR/$OUTPUT_FILE"
          fi
        done
      fi
    done
  done

  echo -en "\r\033[K"
  echo "Results saved to: $OUTPUT_DIR/$OUTPUT_FILE"
}

# executeN1ql
# -----------------------------------
# Execute a N1QL statement
# -----------------------------------
# shellcheck disable=SC2001
executeN1ql() {
  local query_node="${1}"
  local statement="${2}"
  debug "query_node: $query_node"
  debug "statement: $statement"
  # call the nodes endpoint
  local url=$PROTOCOL://$query_node:$QUERY_PORT/query/service
  debug "url: $url"
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
      '.results'
  else
    error "Unable to reach the query_node: ${query_node} at ${url}"
    exit 1
  fi
}

# getSlowQueryStatement
# -----------------------------------
# Build a N1QL statement to identify the slow queries
# -----------------------------------
getSlowQueryStatement() {
  local include_clients="${1}"
  local start_date="${2//none/}"
  local end_date="${3//none/}"
  local sort="${4}"
  debug "
  arguments:
    include_clients: $include_clients
    start_date: $start_date
    end_date: $end_date
    sort: $sort"

  # build a n1ql statement to query system:completed_requests, group all of the requests by the
  # statement or preparedText so the average times and totals can be computed
  local n1ql="
  SELECT SUBSTR(REGEXP_REPLACE(IFMISSING(preparedText, statement), '[\r\n\t]+', ' '), 0, $MAX_CHARS) as statement,
    DURATION_TO_STR(g_avgServiceTime) AS avgServiceTime,
    DURATION_TO_STR(g_minServiceTime) AS minServiceTime,
    DURATION_TO_STR(g_maxServiceTime) AS maxServiceTime,
    ROUND(g_avgServiceTime / 1e6) AS avgServiceTime_ms,
    ROUND(g_minServiceTime / 1e6) AS minServiceTime_ms,
    ROUND(g_maxServiceTime / 1e6) AS maxServiceTime_ms,
    DURATION_TO_STR(g_avgElapsedTime) AS avgElapsedTime,
    DURATION_TO_STR(g_minElapsedTime) AS minElapsedTime,
    DURATION_TO_STR(g_maxElapsedTime) AS maxElapsedTime,
    ROUND(g_avgElapsedTime / 1e6) AS avgElapsedTime_ms,
    ROUND(g_minElapsedTime / 1e6) AS minElapsedTime_ms,
    ROUND(g_maxElapsedTime / 1e6) AS maxElapsedTime_ms,
    DURATION_TO_STR(g_avgQueueTime) AS avgQueueTime,
    DURATION_TO_STR(g_minQueueTime) AS minQueueTime,
    DURATION_TO_STR(g_maxQueueTime) AS maxQueueTime,
    ROUND(g_avgQueueTime / 1e3) AS avgQueueTime_us,
    ROUND(g_minQueueTime / 1e3) AS minQueueTime_us,
    ROUND(g_maxQueueTime / 1e3) AS maxQueueTime_us,
    g_totalQueries AS totalQueries,
    g_avgSizeBytes AS avgSizeBytes,
    g_minSizeBytes AS minSizeBytes,
    g_maxSizeBytes AS maxSizeBytes,
    g_avgResults AS avgResults,
    g_minResults AS minResults,
    g_maxResults AS maxResults,
    g_avgFetches AS avgFetches,
    g_minFetches AS minFetches,
    g_maxFetches AS maxFetches,
    g_avgScanResults AS avgScanResults,
    g_minScanResults AS minScanResults,
    g_maxScanResults AS maxScanResults,
    ROUND(g_selectivity, 2) AS querySelectivity,
    MIN(requestTime) AS firstSeenAt,
    MAX(requestTime) AS lastExecutedAt"

  # check to see if include clients is true, if it is, aggregate all of the client context ids and
  # return them as a single string
  if [[ $include_clients == true ]]; then
    n1ql="$n1ql
    , REGEX_REPLACE(
        ENCODE_JSON(ARRAY_AGG(DISTINCT clientContextID)),
        '\\\\[|\\\\]|\\\"|null',
        ''
      ) AS clientContextIDs
    "
  fi

  # add the keyspace and where predicates to filter out certain statements
  n1ql="$n1ql
  FROM system:completed_requests
  WHERE node = NODE_NAME()
    AND clientContextID NOT LIKE 'INTERNAL-%'
    AND UPPER(IFMISSING(preparedText, statement)) NOT LIKE 'INFER %'
    AND UPPER(IFMISSING(preparedText, statement)) NOT LIKE 'ADVISE %'
    AND UPPER(IFMISSING(preparedText, statement)) NOT LIKE '% INDEX%'
    AND UPPER(IFMISSING(preparedText, statement)) NOT LIKE '% SYSTEM:%'"
  # if there is a start date
  if [[ -n "${start_date/none/ }" ]]; then
    n1ql="$n1ql
    AND requestTime >= \"$start_date\""
  fi
  # if there is a end date
  if [[ -n "${end_date/none/ }" ]]; then
    n1ql="$n1ql
    AND requestTime <= \"$end_date\""
  fi

  # add the group by details
  n1ql="$n1ql
  GROUP BY IFMISSING(preparedText, statement)
  LETTING g_avgServiceTime = AVG(STR_TO_DURATION(serviceTime)),
          g_minServiceTime = MIN(STR_TO_DURATION(serviceTime)),
          g_maxServiceTime = MAX(STR_TO_DURATION(serviceTime)),
          g_avgElapsedTime = AVG(STR_TO_DURATION(elapsedTime)),
          g_minElapsedTime = MIN(STR_TO_DURATION(elapsedTime)),
          g_maxElapsedTime = MAX(STR_TO_DURATION(elapsedTime)),
          g_avgQueueTime = AVG(STR_TO_DURATION(elapsedTime) - STR_TO_DURATION(serviceTime)),
          g_minQueueTime = MIN(STR_TO_DURATION(elapsedTime) - STR_TO_DURATION(serviceTime)),
          g_maxQueueTime = MAX(STR_TO_DURATION(elapsedTime) - STR_TO_DURATION(serviceTime)),
          g_totalQueries = COUNT(1),
          g_avgSizeBytes = AVG(resultSize),
          g_minSizeBytes = MIN(resultSize),
          g_maxSizeBytes = MAX(resultSize),
          g_avgResults = AVG(resultCount),
          g_minResults = MIN(resultCount),
          g_maxResults = MAX(resultCount),
          g_avgFetches = AVG(IFMISSING(phaseCounts.\`fetch\`, 0)),
          g_minFetches = MIN(IFMISSING(phaseCounts.\`fetch\`, 0)),
          g_maxFetches = MAX(IFMISSING(phaseCounts.\`fetch\`, 0)),
          g_avgScanResults = AVG(IFMISSING(phaseCounts.\`indexScan\`, 0)),
          g_minScanResults = MIN(IFMISSING(phaseCounts.\`indexScan\`, 0)),
          g_maxScanResults = MAX(IFMISSING(phaseCounts.\`indexScan\`, 0)),
          g_selectivity = IFNULL(
            (
              AVG(resultCount) /
              AVG(IFMISSING(phaseCounts.\`indexScan\`, 0))
            ) * 100,
          0)
  "

  # add the sort order for the query
  case "$sort" in
    "time")  n1ql="$n1ql ORDER BY g_avgServiceTime DESC"
        ;;
    "queries")  n1ql="$n1ql ORDER BY g_totalQueries DESC"
        ;;
    "size")  n1ql="$n1ql ORDER BY g_avgSizeBytes DESC"
        ;;
    "results") n1ql="$n1ql ORDER BY g_avgResults DESC"
       ;;
    "fetches") n1ql="$n1ql ORDER BY g_avgFetches DESC"
      ;;
    "scans") n1ql="$n1ql ORDER BY g_avgScanResults DESC"
      ;;
    "selectivity") n1ql="$n1ql ORDER BY g_selectivity ASC"
      ;;
    *) n1ql="$n1ql ORDER BY g_avgServiceTime DESC"
       ;;
  esac

  echo "$n1ql"
}

# getColumnHeadings
# -----------------------------------
# Get the list of column headings to use for the report
# -----------------------------------
getColumnHeadings() {
  local include_indexes="${1}"
  local include_clients="${2}"
  local columns='"cluster_name","query_node","statement","firstSeenAt","lastExecutedAt","querySelectivity","avgFetches","minFetches","maxFetches","avgResults","minResults","maxResults","avgScanResults","minScanResults","maxScanResults","avgServiceTime","minServiceTime","maxServiceTime","avgServiceTime_ms","minServiceTime_ms","maxServiceTime_ms","avgElapsedTime","minElapsedTime","maxElapsedTime","avgElapsedTime_ms","minElapsedTime_ms","maxElapsedTime_ms","avgQueueTime","minQueueTime","maxQueueTime","avgQueueTime_us","minQueueTime_us","maxQueueTime_us","avgSizeBytes","minSizeBytes","maxSizeBytes","totalQueries"'
  debug "
  arguments:
    include_indexes: $include_indexes
    include_clients: $include_clients"
  if [[ $include_indexes == true ]]; then
    columns="$columns,""index"""
  fi
  if [[ $include_clients == true ]]; then
    columns="$columns,""clientContextIDs"""
  fi
  echo "$columns"
}

# getQueryNodes
# -----------------------------------
# Gets all of the available query nodes in the cluster
# -----------------------------------
# shellcheck disable=SC2001
getQueryNodes() {
  local cluster="${1}"
  debug "cluster: $cluster"
  local query_nodes
  # call the nodes endpoint
  local url="$PROTOCOL://$cluster:$PORT/pools/nodes"
  debug "url: $url"
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
    query_nodes=$(echo "$http_body" | jq --raw-output --compact-output \
      --argjson input "$http_body" \
      '[
        .nodes[] |
        select(.services | contains(["n1ql"]) == true) |
        .hostname | split(":")[0]
        ] | join(",")
      ')
  else
    error "Unable to reach the cluster: ${cluster} at ${url}"
    exit 1
  fi
  echo "$query_nodes"
}

# getClusterName
# -----------------------------------
# Retrieves the name of the cluster
# -----------------------------------
# shellcheck disable=SC2001
getClusterName() {
  local cluster="${1}"
  debug "cluster: $cluster"
  # call the index status api
  local url="$PROTOCOL://$cluster:$PORT/pools/default"
  debug "url: $url"
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
  debug "http_status: $http_status"
  # inspect the response code
  if [ "$http_status" -eq "200" ]; then
     # parse the response, append the indexes from the cluster to the global indexes variable
     echo "$http_body" | jq --arg cluster "$cluster" --raw-output --compact-output \
      'if (.clusterName | length > 0) then
        .clusterName
      else
        "Not Set for " + $cluster
      end'
  else
    echo "N/A"
  fi
}


# ******************************************************************************************************
# *********************                DO NOT EDIT BELOW THIS LINE                **********************
# ******************************************************************************************************
# Template inspired by:
#  - https://github.com/oxyc/bash-boilerplate/blob/master/script.sh
#  - https://github.com/kvz/bash3boilerplate/blob/master/example.sh

set -o errexit # Exit on error. Append '||true' when you run the script if you expect an error.
set -o errtrace # Exit on error inside any functions or subshells.
set -o pipefail # Exit on piping, bash will remember & return the highest exitcode in a chain of pipes.
set -o nounset # Exit when undeclared variables are used

# magic variables for use within the script
__dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)" # the directory the script is being executed in
__script_path="${__dir}/$(basename "${BASH_SOURCE[0]}")" # the full path to the script
__script="$(basename "${__script_path}")" # the name of the script including the extension
__script_name="$(basename "${__script_path}" .sh)" # the name of the script without the extension
# shellcheck disable=SC2015
__invocation="$(printf %q "${__script_path}")$( (($#)) && printf ' %q' "$@" || true )" # the invocating command and options passed to the script at execution time

# Set Temp Directory
# -----------------------------------
# Create temp directory with three random numbers and the process ID
# in the name.  This directory is removed automatically at exit.
# -----------------------------------
tmp_dir="/tmp/${__script_name}.$RANDOM.$RANDOM.$RANDOM.$$"
(umask 077 && mkdir "${tmp_dir}") || {
  error "Could not create temporary directory! Exiting." && exit 1
}

# _cleanup
# -----------------------------------
# Remove any tmp files, if any
# -----------------------------------
_cleanup() {
  if [ -d "${tmp_dir}" ]; then
    rm -r "${tmp_dir}"
  fi
}

LOG_LEVEL=${LOG_LEVEL:=6} # 7 = debug -> 0 = emergency
NO_COLOR="${NO_COLOR:-}"
TRACE="0"

# _log
# -----------------------------------
# Handles all logging, all log messages are output to stderr so stdout can still be piped
#   Example: _log "info" "Some message"
# -----------------------------------
# shellcheck disable=SC2034
_log () {
  local log_level="${1}" # first option is the level, the rest is the message
  shift
  local color_success="\\x1b[32m"
  local color_debug="\\x1b[36m"
  local color_info="\\x1b[90m"
  local color_notice="\\x1b[34m"
  local color_warning="\\x1b[33m"
  local color_error="\\x1b[31m"
  local color_critical="\\x1b[1;31m"
  local color_alert="\\x1b[1;33;41m"
  local color_emergency="\\x1b[1;4;5;33;41m"
  local colorvar="color_${log_level}"
  local color="${!colorvar:-${color_error}}"
  local color_reset="\\x1b[0m"

  # If no color is set or a non-recognized terminal is used don't use colors
  if [[ "${NO_COLOR:-}" = "true" ]] || { [[ "${TERM:-}" != "xterm"* ]] && [[ "${TERM:-}" != "screen"* ]]; } || [[ ! -t 2 ]]; then
    if [[ "${NO_COLOR:-}" != "false" ]]; then
      color="";
      color_reset="";
    fi
  fi

  # all remaining arguments are to be printed
  local log_line=""

  while IFS=$'\n' read -r log_line; do
    echo -e "$(date +"%Y-%m-%d %H:%M:%S %Z") ${color}[${log_level}]${color_reset} ${log_line}" 1>&2
  done <<< "${@:-}"
}

# emergency
# -----------------------------------
# Handles emergency logging
# -----------------------------------
emergency() {
  _log emergency "${@}"; exit 1;
}

# success
# -----------------------------------
# Handles success logging
# -----------------------------------
success() {
  _log success "${@}"; true;
}

# alert
# -----------------------------------
# Handles alert logging
# -----------------------------------
alert() {
  [[ "${LOG_LEVEL:-0}" -ge 1 ]] && _log alert "${@}";
  true;
}

# critical
# -----------------------------------
# Handles critical logging
# -----------------------------------
critical() {
  [[ "${LOG_LEVEL:-0}" -ge 2 ]] && _log critical "${@}";
  true;
}

# error
# -----------------------------------
# Handles error logging
# -----------------------------------
error() {
  [[ "${LOG_LEVEL:-0}" -ge 3 ]] && _log error "${@}";
  true;
}

# warning
# -----------------------------------
# Handles warning logging
# -----------------------------------
warning() {
  [[ "${LOG_LEVEL:-0}" -ge 4 ]] && _log warning "${@}";
  true;
}

# notice
# -----------------------------------
# Handles notice logging
# -----------------------------------
notice() {
  [[ "${LOG_LEVEL:-0}" -ge 5 ]] && _log notice "${@}";
  true;
}

# info
# -----------------------------------
# Handles info logging
# -----------------------------------
info() {
  [[ "${LOG_LEVEL:-0}" -ge 6 ]] && _log info "${@}";
  true;
}

# debug
# -----------------------------------
# Handles debug logging and prepends the name of the that called debug in front of the message
# -----------------------------------
debug() {
  [[ "${LOG_LEVEL:-0}" -ge 7 ]] && _log debug "${FUNCNAME[1]}() ${*}";
  true;
}

# _exit
# -----------------------------------
# Non destructive exit for when script exits naturally.
# -----------------------------------
_exit() {
  _cleanup
  trap - INT TERM EXIT
  exit
}

# _error_report
# -----------------------------------
# Any actions that should be taken if the script is prematurely exited.
# -----------------------------------
_error_report() {
  _cleanup
  error "Error in ${__script} in ${1} on line ${2}"
  exit 1
}

# trap bad exits with custom _trap function
trap '_error_report "${FUNCNAME:-.}" ${LINENO}' ERR

# Set IFS to preferred implementation
IFS=$'\n\t'

# Iterate over options breaking --foo=bar into --foo bar, and handle common arguments like --debug, --log-level, --no-color
unset options
while (($#)); do
  case $1 in
    # If option is of type --foo=bar
    --?*=*) options+=("${1%%=*}" "${1#*=}") ;;
    --help) _usage >&2; _exit ;;
    --version) echo "${__script_name} ${version}"; _exit ;;
    --log-level) LOG_LEVEL=${2} && shift ;;
    --no-color) NO_COLOR=true ;;
    --debug) LOG_LEVEL="7" ;;
    --trace)
      TRACE="1"
      LOG_LEVEL="7"
    ;;
    # add --endopts for --
    --) options+=(--endopts) ;;
    # Otherwise, nothing special
    *) options+=("$1") ;;
  esac
  shift
done

if [ "${options:-}" != "" ]; then
  set -- "${options[@]}"
  unset options
fi

# parse the options
_options "$@"

# if trace has been set to 1 via the --trace argument enable tracing after the options have been parsed
if [[ "${TRACE}" == "1" ]]
then
  set -o xtrace
fi

# validate the options
_validate

# check dependencies
_dependencies

# call the main function
main

# cleanly exit
_exit
