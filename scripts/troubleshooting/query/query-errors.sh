#!/usr/bin/env bash

version="1.0.0"

_usage() {
  echo -n "${__script} [OPTION]...

 This will output queries with errors.  Note that this queries system:completed_requests and has a predicate of
 WHERE errorCount > 0.  This will not output all queries with errors as for a query to be saved in
 system:completed_requests it must exceed the completed-threshold which by default is 1000ms.

 Options:
  -c, --cluster(s)        A comma-delimited list of one or more clusters to retrieve the slow queries from.  (default: localhost)
  -u, --username          Cluster Admin or RBAC username (default: Administrator)
  -p, --password          Cluster Admin or RBAC password (default: password)
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
QUERY_NODE=${QUERY_NODE:=''}
QUERY_PORT=${QUERY_PORT:='8093'}
OUTPUT_DIR=${OUTPUT_DIR:=$(pwd)}
OUTPUT_FILE="query-errors-$(date +"%Y-%m-%dT%H:%M:%S").csv"
START_DATE=${START_DATE:='none'}
END_DATE=${END_DATE:='none'}

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
      -n|--query-node) QUERY_NODE=${2} && shift 2;;
      -q|--query-port) QUERY_PORT=${2} && shift 2;;
      -d|--start-date) START_DATE=${2} && shift 2;;
      -e|--end-date) END_DATE=${2} && shift 2;;
      -u|--username) USERNAME=${2} && shift 2;;
      -o|--output-dir) OUTPUT_DIR=${2} && shift 2;;
      -f|--output-file) OUTPUT_FILE=${2} && shift 2;;
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
  headings=$(getColumnHeadings)

  # write out the initial headings
  echo "$headings" > "$OUTPUT_DIR/$OUTPUT_FILE"

  # build the slow query n1ql statement to be used
  local n1ql
  n1ql=$(getErrorQueryStatement "$START_DATE" "$END_DATE")

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

      # output the results
      echo "$results" | jq  --raw-output \
        --arg cluster_name "$cluster_name" \
        --arg node "$node" '.[] | [
        $cluster_name,
        $node,
        .statement,
        .elapsed_time,
        .elapsed_time_ms,
        .fetches,
        .query_selectivity_percent,
        .queue_time_ms,
        .request_id,
        .request_time,
        .result_count,
        .result_size,
        .scan_results,
        .service_time,
        .service_time_ms,
        .state
      ] | @csv' >> "$OUTPUT_DIR/$OUTPUT_FILE"
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

# getErrorQueryStatement
# -----------------------------------
# Build a N1QL statement to identify the queries with errors
# -----------------------------------
getErrorQueryStatement() {
  local start_date="${1//none/}"
  local end_date="${2//none/}"
  debug "
  arguments:
    start_date: $start_date
    end_date: $end_date"

  # build a n1ql statement to query system:completed_requests, group all of the requests by the
  # statement or preparedText so the average times and totals can be computed
  local n1ql="
  SELECT IFMISSING(preparedText, statement) as statement, requestId AS request_id,
        serviceTime AS service_time, service_time_ms, elapsedTime AS elapsed_time, elapsed_time_ms,
        queue_time_ms, requestTime AS request_time,
        resultCount AS result_count, resultSize AS result_size,
        query_selectivity_percent,
        scan_results, state, fetches
    FROM system:completed_requests
    LET service_time_ms = ROUND(STR_TO_DURATION(serviceTime) / 1e6),
        elapsed_time_ms = ROUND(STR_TO_DURATION(elapsedTime) / 1e6),
        queue_time_ms = ROUND(
          (STR_TO_DURATION(elapsedTime) - STR_TO_DURATION(serviceTime)) / 1e6
        , 3),
        query_selectivity_percent = ROUND(IFNULL(
          (
            resultCount /
            IFMISSING(phaseCounts.\`indexScan\`, 0)
          ) * 100,
        0), 2),
        scan_results = IFMISSING(phaseCounts.\`indexScan\`, 0),
        fetches = IFMISSING(phaseCounts.\`fetch\`, 0)
    WHERE node = NODE_NAME()
        AND UPPER(IFMISSING(preparedText, statement)) NOT LIKE 'INFER %'
        AND UPPER(IFMISSING(preparedText, statement)) NOT LIKE 'ADVISE %'
        AND UPPER(IFMISSING(preparedText, statement)) NOT LIKE '% INDEX%'
        AND UPPER(IFMISSING(preparedText, statement)) NOT LIKE '% SYSTEM:%'
        AND errorCount > 0
    ORDER BY requestTime DESC"

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

  echo "$n1ql"
}

# getColumnHeadings
# -----------------------------------
# Get the list of column headings to use for the report
# -----------------------------------
getColumnHeadings() {
  debug ""
  local columns='"cluster_name","query_node","statement","elapsed_time","elapsed_time_ms","fetches","query_selectivity_percent","queue_time_ms","request_id","request_time","result_count","result_size","scan_results","service_time","service_time_ms","state"'
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
