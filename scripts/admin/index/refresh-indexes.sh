#!/usr/bin/env bash

version="1.0.0"

_usage() {
  echo -n "${__script} [OPTION]...

  This script will compare the index definitions from source to target and apply all the necessary changes in the target to make the index definitions same. All these changes will be done without any downtime. It identifies and fixes the three major differences.
 	    1. All the indexes that are missing in the target will be created.
 	    2. All the extra indexes present in the target will be removed
      3. All the indexes with difference in the definitions will be recreated with the index definition in the source

  This script will generate index defenition files for source and target clusters. It will optionally genearte a file with all the n1ql statements that will be executed in the target cluster.

 Options:
  -sc, --source-cluster           The source cluster address (default: localhost)
                                  (Optional, if source index defenitions file is provided)
  -tc, --target-cluster           The target cluster address (default: localhost)
  -u, --username          Cluster Admin or RBAC username (default: Administrator)
  -p, --password          Cluster Admin or RBAC password (default: password)
  -f, --source-file       Source Index Definitions File (optional, if source cluster is provided)
  -b, --buckets           Optional - Comma seperated list of buckets
  -o, --output-file       Optional - Output file where all the target n1ql queries are logged
  -rf, --report-file      Optional - Report file where all the differences are reported
  -c, --check             Determines whether to execute the script in checking mode (default: false)
  -r, --port              The port to use (default: 8091)
  -s, --protocol          The protocol to use (default: http)
  -t, --timeout           The timeout to use for HTTP requests (default: 5)
  --log-level             The log level to to use 0-7 (default: 6)
  --debug                 Shortcut for --log-level 7
  --help                  Display this help and exit
  --version               Output version information and exit
"
}

# default variables / flags and their optional corresponding environment variables used in the script
SOURCE_CLUSTER=${SOURCE_CLUSTER:='localhost'}
TARGET_CLUSTER=${TARGET_CLUSTER:='localhost'}
USERNAME=${CB_USERNAME:='Administrator'}
PASSWORD=${CB_PASSWORD:='password'}
SOURCE_INDEX_DEFINITIONS_FILE=${SOURCE_INDEX_DEFINITIONS_FILE:=''}
BUCKETS=${BUCKETS:=''}
OUTPUT_FILE=${OUTPUT_FILE:=''}
REPORT_FILE=${OUTPUT_FILE:=''}
CHECK=${CHECK:='false'}
PORT=${PORT:='8091'}
PROTOCOL=${PROTOCOL:='http'}
TIMEOUT=${TIMEOUT:=5}

# _options
# -----------------------------------
# Parses CLI options
# -----------------------------------
_options() {
  debug ""
  while [[ $# -gt 0 ]]; do
    case "$1" in
    -sc | --source-cluster) SOURCE_CLUSTER=${2} && shift 2 ;;
    -tc | --target-cluster) TARGET_CLUSTER=${2} && shift 2 ;;
    -f | --source-file) SOURCE_INDEX_DEFINITIONS_FILE=${2} && shift 2 ;;
    -b | --buckets) BUCKETS=${2} && shift 2 ;;
    -o | --output-file) OUTPUT_FILE=${2} && shift 2 ;;
    -rf | --report-file) REPORT_FILE=${2} && shift 2 ;;
    -c | --check) CHECK=${2} && shift 2 ;;
    -r | --port) PORT=${2} && shift 2 ;;
    -s | --protocol) PROTOCOL=${2} && shift 2 ;;
    -t | --timeout) TIMEOUT=${2} && shift 2 ;;
    -u | --username) USERNAME=${2} && shift 2 ;;
    -p | --password)
      # if no password was specified prompt for one
      if [[ "${2:-}" == "" || "${2:-}" == --* ]]; then
        stty -echo                       # disable keyboard input
        read -p "Password: " -r PASSWORD # prompt the user for the password
        stty echo                        # enable keyboard input
        echo                             # new line
        tput cuu1 && tput el             # clear the previous line
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
  # check if sqlite is installed
  if [ "$(command -v sqlite3)" = "" ]; then
    emergency "sqlite3 command is required, see (https://www.sqlite.org/download.html)"
  fi
  # check if the dependent files are present
  FILE=./build-indexes.sh
  if [ ! -f "$FILE" ]; then
    emergency "$FILE is required in current path, see (https://github.com/couchbaselabs/scripts/tree/master/external/admin/index)"
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
  if [[ "$SOURCE_CLUSTER" =~ :[0-9]+ ]]; then
    warning "Do not specifiy the port for the hostnames in the -c/--source-clusters argument" && valid=false
  fi
  # validate the cluster argument does not contain any port references
  if [[ "$TARGET_CLUSTER" =~ :[0-9]+ ]]; then
    warning "Do not specifiy the port for the hostnames in the -c/--target-clusters argument" && valid=false
  fi
  # validate the cluster argument does not contain the protocol
  if [[ "$SOURCE_CLUSTER" =~ https?:// ]]; then
    warning "Do not specifiy the protocol (http/https) for the hostnames in the -c/--source-clusters argument" && valid=false
  fi
  # validate the cluster argument does not contain the protocol
  if [[ "$TARGET_CLUSTER" =~ https?:// ]]; then
    warning "Do not specifiy the protocol (http/https) for the hostnames in the -c/--target-clusters argument" && valid=false
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
  # if there are errors
  if (! $valid); then
    exit 1
  fi
}

QUERY_NODE=""
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
    source-cluster: $SOURCE_CLUSTER
    target-cluster: $TARGET_CLUSTER
    source-file: $SOURCE_INDEX_DEFINITIONS_FILE
    output-file: $OUTPUT_FILE
    check: $CHECK
    username: $USERNAME
    password: ********
    port: $PORT
    protocol: $PROTOCOL
    timeout: $TIMEOUT"

  local source_index_definition_file
  local target_index_definition_file
  source_index_definition_file="indexes-src-$SOURCE_CLUSTER-$(date +"%Y-%m-%dT%H:%M:%S").tsv"
  target_index_definition_file="indexes-tgt-$TARGET_CLUSTER-$(date +"%Y-%m-%dT%H:%M:%S").tsv"
  initialize

  if [ -z "$SOURCE_INDEX_DEFINITIONS_FILE" ]; then
    generateIndexDefinitions "$SOURCE_CLUSTER" "$source_index_definition_file"
  else
    source_index_definition_file="$SOURCE_INDEX_DEFINITIONS_FILE"
  fi
  generateIndexDefinitions "$TARGET_CLUSTER" "$target_index_definition_file"

  loadSqlite "tabSrc" "$source_index_definition_file"
  loadSqlite "tabTgt" "$target_index_definition_file"

  createMissingIndexes
  replaceMisMatchingIndexes
  dropExtraIndexes
  success "Refreshing indexes completed successfully"

}
initialize() {

  if [ -n "$OUTPUT_FILE" ]; then
    echo "" >"$OUTPUT_FILE"
    info "The output scripts are getting written to the file: $OUTPUT_FILE"
  fi
  if [ -n "$REPORT_FILE" ]; then
    {
      echo "Index Comparison Report"
      echo "Source Cluster: $SOURCE_CLUSTER"
      echo "Target Cluster: $TARGET_CLUSTER"
      echo ""
    } >"$REPORT_FILE"
    info "The index differences are getting written to the report file: $REPORT_FILE"
  fi
  if [ "$CHECK" = "true" ]; then
    info "Script is running in check mode. No operations will be performed in the target cluster"
  fi
  if [ "$QUERY_NODE" == "" ] || [ -z ${QUERY_NODE+x} ]; then
    query_nodes=$(getQueryNodes "$TARGET_CLUSTER")
  else
    query_nodes="$QUERY_NODE"
  fi
  IFS=', ' read -r -a query_nodes <<<"$query_nodes"
  QUERY_NODE="${query_nodes[$((RANDOM % ${#query_nodes[@]}))]}"
  debug "Query Node: $QUERY_NODE"
}
generateIndexDefinitions() {
  local cluster=$1
  local filename=$2

  if [ -n "$BUCKETS" ]; then
    bucketslist=$(echo "\"$BUCKETS\"" | tr -d " " | sed 's/,/","/g')
    # shellcheck disable=SC2086
    curl --user "$USERNAME:$PASSWORD" --silent http://$cluster:$PORT/indexStatus | jq -r --arg buckets "$bucketslist" '[.indexes[] | select(([.bucket] | inside([$buckets])) and (.index | contains(" (replica ") | not))] | sort_by(.bucket) | .[] | "\(.bucket)\t\(.index)\t\(.definition)"' | sed s/'"nodes":\[[^]]*],'// | sed -e 's/  */ /g' -e 's/^ *\(.*\) *$/\1/' | uniq >"$filename"
  else
    # shellcheck disable=SC2086
    curl --user "$USERNAME:$PASSWORD" --silent http://$cluster:8091/indexStatus | jq -r '.indexes | sort_by(.bucket) | .[] | "\(.bucket)\t\(.index)\t\(.definition)"' | sed s/'"nodes":\[[^]]*],'// | sed -e 's/ (replica [0-9]*)//' | sed -e 's/  */ /g' -e 's/^ *\(.*\) *$/\1/' | uniq >"$filename"
  fi
  info "Index defenitions of cluster $cluster generated at $filename"
}
loadSqlite() {
  local tablename=$1
  local filename=$2
  sqlite3 test.db "DROP TABLE IF EXISTS $tablename"
  sqlite3 test.db "create table $tablename (bucket TEXT,name TEXT,definition TEXT);"
  sqlite3 -separator $'\t' test.db ".import $filename $tablename"
  local recordcount
  recordcount=$(sqlite3 test.db "select count(*) from $tablename")
  info "$recordcount indexes loaded from $filename"
}
createMissingIndexes() {
  local indexes
  local count

  logFile $'##Script for creating Missing Indexes'

  indexes=$(sqlite3 test.db "select tabSrc.definition || \";\" from tabSrc left join tabTgt on tabSrc.bucket = tabTgt.bucket and tabSrc.name = tabTgt.name where tabTgt.bucket is null")
  if [ -z "$indexes" ]; then
    msg="There are no missing indexes in the target cluster"
    logReport "#$msg#"
    success "$msg"
    return
  fi

  count=$(echo "$indexes" | wc -l | sed -e 's/^ *\(.*\) *$/\1/')
  msg="Missing Indexes in Target: $count"
  info "$msg"
  logReport "#$msg#"
  logFile "$indexes"
  #Priniting source index defenitions in report
  if [ -n "$REPORT_FILE" ]; then
    indexesReport=$(sqlite3 test.db 'select "Source Index definition: " || tabSrc.definition from tabSrc left join tabTgt on tabSrc.bucket = tabTgt.bucket and tabSrc.name = tabTgt.name where tabTgt.bucket is null')
    logReport "$indexesReport"
  fi

  executeN1QLQueries "$indexes"

  buildIndexes
  success "Processing $count missing indexes completed"
}
replaceMisMatchingIndexes() {
  logFile $'##Script for replacing Mis-Matching Indexes'
  local indexes
  local count

  #CREATE INDEXES WITH TEMPERORY NAME
  indexes=$(sqlite3 test.db 'select REPLACE(tabSrc.definition,tabSrc.name,tabSrc.name || "_temp") || ";" from tabSrc inner join tabTgt on tabSrc.bucket = tabTgt.bucket and tabSrc.name = tabTgt.name where tabSrc.definition != tabTgt.definition')
  if [ -z "$indexes" ]; then
    msg="There are no mis-matching indexes in the target cluster"
    logReport "#$msg#"
    success "$msg"
    return
  fi

  count=$(echo "$indexes" | wc -l | sed -e 's/^ *\(.*\) *$/\1/')
  msg="Mismatching Indexes in Target: $count"
  info "$msg"
  logReport "#$msg#"
  logFile "$indexes"
  #Priniting source and target index defenitions in report
  if [ -n "$REPORT_FILE" ]; then
    indexesReport=$(sqlite3 test.db 'select "Source Index definition: " || tabSrc.definition || "\n" || "Target Index definition :" || tabTgt.definition from tabSrc inner join tabTgt on tabSrc.bucket = tabTgt.bucket and tabSrc.name = tabTgt.name where tabSrc.definition != tabTgt.definition')
    logReport "$indexesReport"
  fi

  executeN1QLQueries "$indexes"

  buildIndexes

  #DROP ORIGINAL INDEXES
  # shellcheck disable=SC2016
  indexes=$(sqlite3 test.db 'select "DROP INDEX `" || tabSrc.bucket || "`.`" || tabSrc.name || "`;" from tabSrc inner join tabTgt on tabSrc.bucket = tabTgt.bucket and tabSrc.name = tabTgt.name where tabSrc.definition != tabTgt.definition')
  logFile "$indexes"
  count=$(echo "$indexes" | wc -l | sed -e 's/^ *\(.*\) *$/\1/')
  debug "Drop Indexes count: $count"
  executeN1QLQueries "$indexes"
  if [ "$CHECK" = "false" ]; then
    debug 'Taking a deep breath to refresh the indexes'
    sleep 5
  fi

  #CREATE INDEXES WITH ORIGINAL NAME
  indexes=$(sqlite3 test.db "select tabSrc.definition || \";\" from tabSrc inner join tabTgt on tabSrc.bucket = tabTgt.bucket and tabSrc.name = tabTgt.name where tabSrc.definition != tabTgt.definition")
  logFile "$indexes"
  count=$(echo "$indexes" | wc -l | sed -e 's/^ *\(.*\) *$/\1/')
  debug "Create Indexes count: $count"
  executeN1QLQueries "$indexes"

  buildIndexes

  #DROP TEMPERORY INDEXES
  # shellcheck disable=SC2016
  indexes=$(sqlite3 test.db 'select "DROP INDEX `" || tabSrc.bucket || "`.`" || tabSrc.name || "_temp`" || ";" from tabSrc inner join tabTgt on tabSrc.bucket = tabTgt.bucket and tabSrc.name = tabTgt.name where tabSrc.definition != tabTgt.definition')
  logFile "$indexes"
  count=$(echo "$indexes" | wc -l | sed -e 's/^ *\(.*\) *$/\1/')
  debug "Drop Temp Indexes count: $count"
  executeN1QLQueries "$indexes"

  success "Processing $count mis-matching indexes completed"
}
dropExtraIndexes() {
  logFile $'##Script for deleting Extra Indexes'
  local indexes
  local count

  # shellcheck disable=SC2016
  indexes=$(sqlite3 test.db 'select "DROP INDEX `" || tabTgt.bucket || "`.`" || tabTgt.name || "`;" from tabTgt left join tabSrc on tabSrc.bucket = tabTgt.bucket and tabSrc.name = tabTgt.name where tabSrc.bucket is null')
  if [ -z "$indexes" ]; then
    msg="There are no extra indexes in the target cluster"
    logReport "#$msg#"
    success "$msg"
    return
  fi

  count=$(echo "$indexes" | wc -l | sed -e 's/^ *\(.*\) *$/\1/')
  msg="Extra Indexes in Target: $count"
  info "$msg"
  logReport "#$msg#"
  logFile "$indexes"
  #Priniting target index defenitions in report
  if [ -n "$REPORT_FILE" ]; then
    indexesReport=$(sqlite3 test.db 'select "Target Index definition :" || tabTgt.definition from tabTgt left join tabSrc on tabSrc.bucket = tabTgt.bucket and tabSrc.name = tabTgt.name where tabSrc.bucket is null')
    logReport "$indexesReport"
  fi

  executeN1QLQueries "$indexes"

  success "Processing $count extra indexes completed"
}
executeN1QLQueries() {
  local queries=$1
  for query in $queries; do
    debug "Executing the query: $query"
    if [ "$CHECK" = "false" ]; then
      #cbq -e "couchbase://$QUERY_NODE" -u "$USERNAME" -p "$PASSWORD" --script="$query"
      executeN1ql "$query"
      Wait
    else
      debug "#CHECK MODE# Skipping query execution"
    fi
  done
  if [ "$CHECK" = "false" ]; then
    debug 'Taking a deep breath to refresh the indexes'
    sleep 10
  fi
  debug "All queries got executed"
}
buildIndexes() {
  logFile "build-indexes.sh --cluster=$TARGET_CLUSTER --username=USERNAME --password=PASSWORD --query-node=$QUERY_NODE"
  debug "build-indexes.sh --cluster=$TARGET_CLUSTER --username=$USERNAME --password=$PASSWORD --query-node=$QUERY_NODE"
  if [ "$CHECK" = "false" ]; then
    # shellcheck disable=SC1091
    result=$(source build-indexes.sh --cluster="$TARGET_CLUSTER" --username="$USERNAME" --password="$PASSWORD" --query-node="$QUERY_NODE")
    debug "$result"
    Wait
  else
    debug "#CHECK MODE# Skipping Index Build"
  fi

  waitForBuildCompletion

}
waitForBuildCompletion() {
  logFile $'##WAIT FOR BUILD COMPLETION##'
  if [ "$CHECK" = "false" ]; then
    debug 'Taking a deep breath to refresh the indexes'
    sleep 5
    printf "Index Build in progress"
    while true; do
      out=$(curl \
        --user "$USERNAME:$PASSWORD" \
        --silent \
        "$PROTOCOL://$TARGET_CLUSTER:$PORT/indexStatus" |
        jq -r '.indexes | map(select((.status != "Ready"))) | .[] | .bucket + ": " + .index + " (" +.status + ")"')
      if [[ -n "$out" ]]; then
        printf .
        sleep 5
        continue
      else
        printf "\nIndex build complete"
        break
      fi
    done
    echo
  else
    debug "#CHECK MODE# Skipping wait for build completion"
  fi
}
logFile() {
  message=$1
  if [ -n "$OUTPUT_FILE" ] && [ -n "$message" ]; then
    {
      echo "$message"
      echo ""
    } >>"$OUTPUT_FILE"
  fi
}
logReport() {
  message=$1
  if [ -n "$REPORT_FILE" ] && [ -n "$message" ]; then
    {
      echo -e "$message"
      echo ""
    } >>"$REPORT_FILE"
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
  debug "Getting Query nodes for cluster $cluster"
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
    debug "$http_response"
    error "Failed to get the query nodes from the cluster: ${cluster} at ${url}"
    exit 1
  fi

  echo "$query_nodes"
}
# -----------------------------------
# Execute a N1QL statement
# -----------------------------------
# shellcheck disable=SC2001

executeN1ql() {
  local statement="${1}"

  # call the nodes endpoint
  local url="$PROTOCOL://$QUERY_NODE:8093/query/service"

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
    # parse the response, and getting the execution metrics
    out=$(echo "$http_body" | jq --raw-output --compact-output \
      '.metrics')
    debug "$out"
  else
    debug "$http_response"
    error "Failed to execute the n1ql query on query_node: ${QUERY_NODE} at ${url}"
    read -p "Do you want to proceed with the execution of the script (Y/N)? " -n 1 -r
    echo
    # if Not "Y" then exit
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
      exit 1
    fi
  fi
}

# ******************************************************************************************************
# *********************                DO NOT EDIT BELOW THIS LINE                **********************
# ******************************************************************************************************
# Template inspired by:
#  - https://github.com/oxyc/bash-boilerplate/blob/master/script.sh
#  - https://github.com/kvz/bash3boilerplate/blob/master/example.sh

set -o errexit  # Exit on error. Append '||true' when you run the script if you expect an error.
set -o errtrace # Exit on error inside any functions or subshells.
set -o pipefail # Exit on piping, bash will remember & return the highest exitcode in a chain of pipes.
set -o nounset  # Exit when undeclared variables are used

# magic variables for use within the script
__dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"    # the directory the script is being executed in
__script_path="${__dir}/$(basename "${BASH_SOURCE[0]}")" # the full path to the script
__script="$(basename "${__script_path}")"                # the name of the script including the extension
__script_name="$(basename "${__script_path}" .sh)"       # the name of the script without the extension
# shellcheck disable=SC2015
__invocation="$(printf %q "${__script_path}")$( (($#)) && printf ' %q' "$@" || true)" # the invocating command and options passed to the script at execution time

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
_log() {
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
      color=""
      color_reset=""
    fi
  fi

  # all remaining arguments are to be printed
  local log_line=""

  while IFS=$'\n' read -r log_line; do
    echo -e "$(date +"%Y-%m-%d %H:%M:%S %Z") ${color}[${log_level}]${color_reset} ${log_line}" 1>&2
  done <<<"${@:-}"
}

# emergency
# -----------------------------------
# Handles emergency logging
# -----------------------------------
emergency() {
  _log emergency "${@}"
  exit 1
}

# success
# -----------------------------------
# Handles success logging
# -----------------------------------
success() {
  _log success "${@}"
  true
}

# alert
# -----------------------------------
# Handles alert logging
# -----------------------------------
alert() {
  [[ "${LOG_LEVEL:-0}" -ge 1 ]] && _log alert "${@}"
  true
}

# critical
# -----------------------------------
# Handles critical logging
# -----------------------------------
critical() {
  [[ "${LOG_LEVEL:-0}" -ge 2 ]] && _log critical "${@}"
  true
}

# error
# -----------------------------------
# Handles error logging
# -----------------------------------
error() {
  [[ "${LOG_LEVEL:-0}" -ge 3 ]] && _log error "${@}"
  true
}

# warning
# -----------------------------------
# Handles warning logging
# -----------------------------------
warning() {
  [[ "${LOG_LEVEL:-0}" -ge 4 ]] && _log warning "${@}"
  true
}

# notice
# -----------------------------------
# Handles notice logging
# -----------------------------------
notice() {
  [[ "${LOG_LEVEL:-0}" -ge 5 ]] && _log notice "${@}"
  true
}

# info
# -----------------------------------
# Handles info logging
# -----------------------------------
info() {
  [[ "${LOG_LEVEL:-0}" -ge 6 ]] && _log info "${@}"
  true
}

# debug
# -----------------------------------
# Handles debug logging and prepends the name of the that called debug in front of the message
# -----------------------------------
debug() {
  [[ "${LOG_LEVEL:-0}" -ge 7 ]] && _log debug "${FUNCNAME[1]}() ${*}"
  true
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
  --help)
    _usage >&2
    _exit
    ;;
  --version)
    echo "${__script_name} ${version}"
    _exit
    ;;
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
if [[ "${TRACE}" == "1" ]]; then
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

#Use of this source code is governed by Apache 2.0 license that can be found in the LICENSE file or at
#https://opensource.org/licenses/Apache-2.0.
