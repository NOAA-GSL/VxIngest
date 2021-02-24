#!/usr/bin/env bash

version="1.0.0"

_usage() {
  echo -n "${__script} [OPTION]...

 This will execute a query N number of times and output all of the indexes used across
 all of the executions.  This is useful for determining if a query is bouncing between
 multiple indexes.

 Options:
  -c, --cluster           The cluster to execute the query against.  (default: localhost)
  -u, --username          Cluster Admin or RBAC username (default: Administrator)
  -p, --password          Cluster Admin or RBAC password (default: password)
  -s, --statement         (optional) The N1QL statement to execute
  -f, --file              (optional) A text file containing a N1QL statement to execute
  -e, --executions        (optional) The number of times to execute the statement (default: 10)
  -r, --port              (optional) The port to use (default: 8091)
  -l, --protocol          (optional) The protocol to use (default: http)
  -t, --timeout           (optional) The timeout to use for HTTP requests (default: 5)
  -n, --query-node        (optional) A comma-delimited list of query nodes to use. If not specified all are used. (default: none)
                          This should only be used on a local machine, if Couchbase is being ran inside of docker,
                          and only a single cluster is being used.
  -q, --query-port        (optional) The query port to use (default: 8093)
  --log-level             The log level to to use 0-7 (default: 6)
  --debug                 Shortcut for --log-level 7
  --help                  Display this help and exit
  --version               Output version information and exit
"
}

# default variables / flags and their optional corresponding environment variables used in the script
CLUSTER=${CLUSTER:='localhost'}
USERNAME=${CB_USERNAME:='Administrator'}
PASSWORD=${CB_PASSWORD:='password'}
PORT=${PORT:='8091'}
PROTOCOL=${PROTOCOL:='http'}
TIMEOUT=${TIMEOUT:=5}
QUERY_NODE=${QUERY_NODE:=''}
QUERY_PORT=${QUERY_PORT:='8093'}
STATEMENT=${STATEMENT:=''}
EXECUTIONS=${EXECUTIONS:='10'}

# _options
# -----------------------------------
# Parses CLI options
# -----------------------------------
_options() {
  debug ""
  while [[ $# -gt 0 ]]; do
    case "$1" in
      -c|--cluster) CLUSTER=${2} && shift 2;;
      -r|--port) PORT=${2} && shift 2;;
      -l|--protocol) PROTOCOL=${2} && shift 2;;
      -t|--timeout) TIMEOUT=${2} && shift 2;;
      -s|--statement) STATEMENT=${2} && shift 2;;
      -f|--file) STATEMENT_FILE=${2} && shift 2;;
      -e|--executions) EXECUTIONS=${2} && shift 2;;
      -n|--query-node) QUERY_NODE=${2} && shift 2;;
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
  if [[ "$CLUSTER" =~ :[0-9]+ ]]; then
    warning "Do not specifiy the port for the hostnames in the -c/--clusters argument" && valid=false
  fi
  # validate the cluster argument does not contain the protocol
  if [[ "$CLUSTER" =~ https?:// ]]; then
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
  # validate the executions argument is a number
  if ! [[ "$EXECUTIONS" =~ ^[1-9][0-9]*$ ]]; then
    warning "The -e/--statement argument must be an integer greater than 0" && valid=false
  fi
  # if a statement file was passed, ensure the file exists
  # shellcheck disable=SC2237
  if ! [ -z ${STATEMENT_FILE+x} ] && ! [ -f "$STATEMENT_FILE" ]; then
    warning "The -f/--file argument contains a path to an invalid file" && valid=false
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
    username: $USERNAME
    password: ********
    statement: $STATEMENT
    executions: $EXECUTIONS"

  # local variable to hold all of the found indexes
  local indexes=""
  # local variable to hold the query node to be used
  local query_nodes
  # if a query node was not passed (and it shouldn't be) get all of the available query
  # nodes in the cluster
  if [ "$QUERY_NODE" == "" ] || [ -z ${QUERY_NODE+x} ]; then
    query_nodes=$(getQueryNodes "$CLUSTER")
  else
    query_nodes="$QUERY_NODE"
  fi
  debug "query_nodes: $query_nodes"
  # convert the query nodes list into an array
  IFS=', ' read -r -a query_nodes <<< "$query_nodes"

  # set the statement if passed from a file
  # shellcheck disable=SC2237
  if ! [ -z ${STATEMENT_FILE+x} ]; then
    STATEMENT=$(cat "$STATEMENT_FILE")
    debug "  statement: $STATEMENT"
  fi

  # loop until the number of executions has been reached
  for (( c=1; c<=EXECUTIONS; c++ ))
  do
    # execute an explain for the query
    local explain
    explain=$(executeN1ql "${query_nodes[$((RANDOM % ${#query_nodes[@]}))]}" "EXPLAIN $STATEMENT") 2>&1 || echo ""
    # pull any "index":"*" statements from the explain
    local query_indexes
    query_indexes=$(echo "$explain" | (grep -E -o  '"index":"[^"]+"' 2>&1) | (tr '\n' ', ' 2>&1 || echo -n "") | (sed -E 's/"index":"//g' 2>&1 || echo -n "") | (sed -E 's/",/, /g' 2>&1 || echo -n "") | (sed -E 's/, $//' 2>&1 || echo -n "") | (tr ", " '\n')) || echo ""
    indexes=$(echo "${indexes}"$'\n'"${query_indexes}" | sed '/^[[:space:]]*$/d')

    # output to the console
    output="Execution: $c of $EXECUTIONS"
    echo -en "\r\033[K$output"
    #echo "${indexes}"
  done
  echo -en "\r\033[KThe following indexes have been found to be used by the query:"
  echo ""
  echo "${indexes}"  | sort | uniq -c | sort -nr

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
