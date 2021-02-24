#!/usr/bin/env bash
# shellcheck disable=SC2155
version="1.0.0"

_usage() {
  echo -n "${__script} [OPTION]...

 Calculates the index cardinality and selectivity for all indexes in the cluster,
 optionally filtering on buckets.

   - Cardinality:            The total number of unique values in the index
   - Selectivity:            Measure of unique values in the dataset calculated as
                             selectivity = cardinality/(number of records) * 100
   - Bucket Selectivity: This is a measure of the # of documents in the bucket that match the index
                             filter/WHERE predicate and contain the leading field.  This is often
                             referred to as  'index segmentation'
   - Index Selectivity:      This is a measure of the number of unique values in the index compared to the
                             total # of entries in the index.

 For optimum performance, you will want a relatively low percentage of Bucket Selectivity as this
 means the index is smaller, and a higher value for Index Selectivity as this means there is a lot of
 uniqueness within the index.

 Options:
  -c, --cluster           Cluster IP Address or Hostname
  -u, --username          Cluster Admin or RBAC username (default: Administrator)
  -p, --password          Cluster Admin or RBAC password (default: password)
  -b, --buckets           (optional) A comma-delimited list of buckets to filter on (default:*)
  -i, --indexes           (optional) A comma-delimited list of indexes to filter on (default:*)
  -r, --port              (optional) The port to use (default: 8091)
  -s, --protocol          (optional) The protocol to use (default: http)
  -n, --query-node        (optional) Query IP Address or Hostname,
                          if not provided a random query node is found in the cluster
  -q, --query-port        (optional) The query port to use (default: 8093)
  -t, --timeout           (optional) The timeout to use for HTTP requests (default: 30)
  --log-level             (optional) The log level to to use 0-7 (default: 6)
  --debug                 Shortcut for --log-level 7
  --help                  Display this help and exit
  --version               Output version information and exit
"
}

# default variables / flags and their optional corresponding environment variables used in the script
CLUSTER=${CLUSTER:='localhost'}
USERNAME=${CB_USERNAME:='Administrator'}
PASSWORD=${CB_PASSWORD:='password'}
BUCKETS=${BUCKETS:='*'}
INDEXES=${INDEXES:='*'}
PORT=${PORT:='8091'}
PROTOCOL=${PROTOCOL:='http'}
TIMEOUT=${TIMEOUT:=30}
QUERY_NODE=${QUERY_NODE:=''}
QUERY_PORT=${QUERY_PORT:='8093'}

# _options
# -----------------------------------
# Parses CLI options
# -----------------------------------
_options() {
  debug ""
  # Read the options and set stuff
  while [[ $# -gt 0 ]]; do
    case "$1" in
      -c|--cluster) CLUSTER=${2} && shift 2;;
      -b|--buckets) BUCKETS=${2} && shift 2;;
      -i|--indexes) INDEXES=${2} && shift 2;;
      -r|--port) PORT=${2} && shift 2;;
      -s|--protocol) PROTOCOL=${2} && shift 2;;
      -t|--timeout) TIMEOUT=${2} && shift 2;;
      -n|--query-node) QUERY_NODE=${2} && shift 2;;
      -q|--query-port) QUERY_PORT=${2} && shift 2;;
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
  if [ "$(command -v jq)" = "" ] ;then
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
  if [[ "$CLUSTER" =~ :[0-9]+ ]]; then
    warning "Do not specifiy the port for the hostnames in the -c/--cluster argument" && valid=false
  fi
  # validate the cluster argument does not contain the protocol
  if [[ "$CLUSTER" =~ https?:// ]]; then
    warning "Do not specifiy the protocol (http/https) for the hostnames in the -c/--cluster argument" && valid=false
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
  # validate the query_node argument does not contain any port references
  if [[ "$QUERY_NODE" =~ :[0-9]+ ]]; then
    warning "Do not specifiy the port for the hostnames in the -n/--query-node argument" && valid=false
  fi
  # validate the query_node argument does not contain the protocol
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
    cluster: $CLUSTER
    buckets: $BUCKETS
    indexes: $INDEXES
    username: $USERNAME
    password: ********
    port: $PORT
    protocol: $PROTOCOL
    timeout: $TIMEOUT
    query_node: $QUERY_NODE
    query_port: $QUERY_PORT"
  # local variable to hold all of the indexes in the cluster
  local indexes=$(getIndexes "$CLUSTER" "$BUCKETS" "$INDEXES")
  # local variable to hold the query node to be used
  local query_node=$(getQueryNode "$CLUSTER")

  # loop over each of the indexes, and calculate it's cardinality and selectivity
  for index in $(echo "$indexes" | jq --raw-output -c '.[]')
  do
    debug "\n  $index"
    # pull out each of the values from the index object
    local current_index=$(echo "$index" | jq --raw-output '.index')
    local current_bucket=$(echo "$index" | jq --raw-output '.bucket')
    local current_is_partitioned=$(echo "$index" | jq --raw-output '.is_partitioned')
    local current_is_array=$(echo "$index" | jq --raw-output '.is_array')
    local current_predicate=$(echo "$index" | jq --raw-output '.predicate')
    local current_leading_field=$(echo "$index" | jq --raw-output '.leading_field')
    local current_definition=$(echo "$index" | jq --raw-output '.definition')
    # if it is not an array index, determine the cardinality and selectivity
    if [[ $current_is_array == false ]]; then
      # output the index label
      if [[ $current_is_partitioned == true ]]; then
        echo "$current_index (partitioned)"
      else
        echo "$current_index"
      fi
      echo "-----------------------------------------------------------------"
      # build a n1ql statement that will retrieve the bucket item count, index count,
      # cardinality, index selectivity and bucket selectivity
      local n1ql="
      SELECT b.itemCount, b.indexCount, b.cardinality,
        TOSTRING(ROUND((b.cardinality / b.indexCount) * 100, 5)) || '%' AS indexSelectivity,
        TOSTRING(ROUND((b.indexCount / b.itemCount) * 100, 5)) || '%' AS bucketSelectivity
      FROM (
        SELECT
          (
            SELECT RAW COUNT(1)
            FROM \`$current_bucket\`
          )[0] AS itemCount,
          (
            SELECT RAW COUNT(1)
            FROM \`$current_bucket\` USE INDEX ($current_index)
            WHERE $current_predicate $current_leading_field IS NOT MISSING
          )[0] AS indexCount,
          (
            SELECT RAW COUNT(DISTINCT $current_leading_field)
            FROM \`$current_bucket\` USE INDEX ($current_index)
            WHERE $current_predicate $current_leading_field IS NOT MISSING
          )[0] AS cardinality
      ) AS b
      "
      # execute the n1ql statement, pass the n1ql statement by stripping line breaks
      local results=$(executeN1ql "$query_node" "${n1ql//$'\n'/}")
      # output the results
      echo "$results" | jq -r --arg index "$current_index" \
        --arg definition "$current_definition" \
        --arg bucket "$current_bucket" \
        --arg is_partitioned "$current_is_partitioned" \
        --arg leading_field "$current_leading_field" '
        "  Bucket: " + $bucket + "\n" +
        "  Bucket Count: " + (.itemCount | tostring) + "\n" +
        "  Leading Field: " + $leading_field + "\n" +
        "  Index Count: " + (.indexCount | tostring) + "\n" +
        "  Cardinality: " + (.cardinality | tostring) + "\n" +
        "  Index Selectivity: " + (.indexSelectivity//0 | tostring) + "\n" +
        "  Bucket Selectivity: " + (.bucketSelectivity | tostring) + "\n" +
        "  Definition: " + $definition + "\n"
      '
    elif  [[ $current_is_partitioned == true ]]; then
      echo "$current_index"
      echo "-----------------------------------------------------------------"
      echo "  N/A Partioned Index"
      echo ""
    else
      echo "$current_index"
      echo "-----------------------------------------------------------------"
      echo "  N/A Array Index"
      echo ""
    fi
  done
  exit;
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
      --argjson input "$http_body" \
      '.results[0]'
  else
    error "Unable to reach the query_node: ${query_node} at ${url}"
    exit 1
  fi
}

# getQueryNode
# -----------------------------------
# Find a query node in the cluster to use
# -----------------------------------
# shellcheck disable=SC2001
getQueryNode() {
  local cluster="${1}"
  debug "cluster: $cluster"
  if [[ "$QUERY_NODE" == "" ]]
  then
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
      QUERY_NODE=$(echo "$http_body" | jq --raw-output --compact-output \
        --argjson input "$http_body" \
        '[
          .nodes[] |
          select(.services | contains(["n1ql"]) == true) |
          .hostname | split(":")[0]
          ][0]
        ')
    else
      error "Unable to reach the cluster: ${cluster} at ${url}"
      exit 1
    fi
  fi
  echo "$QUERY_NODE"
}

# getIndexes
# -----------------------------------
# Retrieves the indexes for a cluster
# -----------------------------------
# shellcheck disable=SC2001
getIndexes() {
  local cluster="${1}"
  local filter_buckets="${2}"
  local filter_indexes="${3}"
  debug "cluster: $cluster"
  debug "filter_buckets: $filter_buckets"
  debug "filter_indexes: $filter_indexes"
  # call the index status api
  local url="$PROTOCOL://$cluster:$PORT/indexStatus"
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
    echo "$http_body" | jq --raw-output --compact-output \
      --arg filter_buckets "$filter_buckets" \
      --arg filter_indexes "$filter_indexes" \
      --argjson input "$http_body" \
      '.indexes as $indexes |
      $filter_buckets | gsub("\\*"; "") | split(",") as $filter_buckets |
      $filter_indexes | gsub("\\*"; "") | split(",") as $filter_indexes |
      $indexes |
        [ .[] | . as $current |
          select(
            (($filter_buckets | length == 0) or (reduce $filter_buckets[] as $item (false; if (. == false and $item == $current.bucket) then . = true else . end)))
            and
            (($filter_indexes | length == 0) or (reduce $filter_indexes[] as $item (false; if (. == false and $item == $current.index) then . = true else . end)))
            and
            ($current.index | contains("replica ") | not)
            and
            ($current.definition | contains("PRIMARY ") | not)
            and
            ($current.status == "Ready")
          ) | {
            "index": $current.index,
            "bucket": $current.bucket,
            # determine whether or not the index is partitioned, as pushdowns cant be leveraged which could result in long running queries
            "is_partitioned": ($current.partitioned),
            # determine if the index is an array index, as pushdowns cant be leveraged which could result in long running queries
            "is_array": ($current.definition | contains("array ") or contains("DISTINCT ")),
            # get the leading field of the index
            "leading_field": ($current.definition | gsub("^CREATE INDEX[^\\(]+\\(|,.+$|\\).*$"; "")),
            # get the index predicate
            "predicate": ($current.definition | gsub("^CREATE INDEX.+ WHERE "; "") | gsub(" WITH \\{.+$"; "") | gsub("^CREATE INDEX.+"; "")),
            # strip off the WITH {...}
            "definition": ($current.definition | gsub(" WITH \\{.+$"; ""))
          } |
          # check to see if the index did contain a predicate, if so append "AND" so it can be used later
          . += { "predicate": (if (.predicate | length != 0) then .predicate + " AND " else "" end )}
        ] | unique
      '
  else
    error "Unable to reach the cluster: ${cluster} at ${url}"
    exit 1
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
