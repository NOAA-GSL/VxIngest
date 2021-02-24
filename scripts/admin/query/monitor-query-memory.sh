#!/usr/bin/env bash

version="1.0.0"

_usage() {
  echo -n "${__script} [OPTION]...

 This script will run at a specified interval indefinitly, checking the memory usage for each query node in the cluster.  If
 the query node exceeds the specified threshold the query node will be auto-failed over and monitored until the memory drops
 below a threshold, at which time it will be recovered and added back into the cluster.

 Options:
  -c, --cluster           The cluster to execute the query against.  (default: localhost)
  -u, --username          Cluster Admin or RBAC username (default: Administrator)
  -p, --password          Cluster Admin or RBAC password (default: password)
  -w, --mem-high-wat      (optional) A high watermark percentage that if exceeded will trigger a failover.  (default: 80)
  -a, --mem-low-wat       (optional) A low watermark percentage that once utilization drops below the node will be recovered.  (default: 30)
  -i, --interval          (optional) An interval in seconds for the amount of time to wait between checks  (default: 60)
  -r, --port              (optional) The port to use (default: 8091)
  -l, --protocol          (optional) The protocol to use (default: http)
  -t, --timeout           (optional) The timeout to use for HTTP requests (default: 5)
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
MEM_HIGH_WAT=${MEM_HIGH_WAT:=80}
MEM_LOW_WAT=${MEM_LOW_WAT:=30}
INTERVAL=${INTERVAL:=60}

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
      -w|--mem-high-wat) MEM_HIGH_WAT=${2} && shift 2;;
      -a|--mem-low-wat) MEM_LOW_WAT=${2} && shift 2;;
      -i|--interval) INTERVAL=${2} && shift 2;;
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
  # validate the mem_high_wat is between 1-99
  if ! [[ "$MEM_HIGH_WAT" =~ ^[1-9][0-9]?$ ]]; then
    warning "The -w/--mem-high-wat argument must be between 1-99" && valid=false
  fi
  # validate the mem_low_wat is between 1-99
  if ! [[ "$MEM_LOW_WAT" =~ ^[1-9][0-9]?$ ]]; then
    warning "The -a/--mem-low-wat argument must be between 1-99" && valid=false
  fi
  #validate that the high-water-mark is not less than the low-water-mark
  if [ "$MEM_HIGH_WAT" -lt "$MEM_LOW_WAT" ]; then
    warning "The -w/--mem-high-wat argument cannot be less than the -a/--mem-low-wat argument" && valid=false
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
    mem_high_wat: $MEM_HIGH_WAT
    mem_low_wat: $MEM_LOW_WAT
    interval: $INTERVAL"
  # local variable to hold the cluster name
  local cluster_name
  cluster_name=$(getClusterName "$CLUSTER")
  info "Starting to monitor cluster ($cluster_name) using the host $CLUSTER"
  info "  Memory High-Watermark: $MEM_HIGH_WAT"
  info "  Memory Low-Watermark: $MEM_LOW_WAT"
  info "  Interval: $INTERVAL"

  # define local variables
  local rebalance_status
  local result
  local node
  local otp_node
  local otp_nodes
  local percent_mem_used
  local failed_node
  local failed_nodes_count

  # loop indefinitely, sleeping the specified interval
  while true
  do
    rebalance_status=$(rebalanceStatus "$CLUSTER")
    # check to see if a rebalance is already, if so skip and sleep
    if [ "$rebalance_status" == "notRunning" ]; then
      # local variable to hold the query node to be used
      local query_nodes
      query_nodes=$(getQueryNodes "$CLUSTER")
      debug "query_nodes: $query_nodes"

      # check to see if there are currently any failed over nodes before starting checks
      failed_nodes_count=$(echo "$query_nodes" | jq -r '
        [
          .[] |
          select(.cluster_membership != "active") |
          .hostname
        ] | length')
      debug "  failed_nodes_count: $failed_nodes_count"

      if [ "$failed_nodes_count" -gt 0 ]; then
        warning "There is 1 or more nodes failed over in the cluster, skipping checks..."
        # check the failed over query nodes and recover them if they have exceeded the low watermark
        failed_node=$(echo "$query_nodes" | jq -r '
          [
            .[] |
            select(.cluster_membership != "active") |
            .
          ][0]')
        # get the node
        node=$(echo "$failed_node" | jq --raw-output '.node')
        # get the otp_node
        otp_node=$(echo "$failed_node" | jq --raw-output '.otp_node')
        # get the percent_mem_free
        percent_mem_used=$(echo "$failed_node" | jq --raw-output '.percent_mem_used')
        # check to see if the current memory used on the node is less than the low water mark
        if [ "$percent_mem_used" -lt "$MEM_LOW_WAT" ]; then
          info "Node: $node has dropped below $MEM_LOW_WAT% and can now be recovered"
          # recover the node and rebalance it back into the cluster
          result=$(recoverNode "$CLUSTER" "$otp_node")
          # get a list of all of the nodes in the cluster
          otp_nodes=$(getOTPNodes "$CLUSTER")
          # perform a rebalance
          info "Rebalancing"
          result=$(rebalance "$CLUSTER" "$otp_nodes")
        fi
      else
        # loop over each of the query nodes
        for row in $(echo "$query_nodes" | jq --raw-output '.[] | @base64'); do
          _jq() {
           echo "$row" | base64 --decode | jq -r "${1}"
          }

          # parse the individual row
          result=$(_jq '.')
          # get the node
          node=$(echo "$result" | jq --raw-output '.node')
          # get the otp_node
          otp_node=$(echo "$result" | jq --raw-output '.otp_node')
          # get the percent_mem_free
          percent_mem_used=$(echo "$result" | jq --raw-output '.percent_mem_used')
          info "Node: $node - Memory Used: $percent_mem_used%"
          # check to see if the percentage of memory used is greater than the high-watermark
          if [ "$percent_mem_used" -gt "$MEM_HIGH_WAT" ]; then
            warning "Node: $node has exceed the memory utilization threshold of $MEM_HIGH_WAT%"
            result=$(failoverNode "$CLUSTER" "$otp_node")
            break 1
          fi
        done
      fi
    else
      warning "There is a rebalance in progress for the $cluster_name ($CLUSTER) cluster, skipping checks..."
    fi
    # sleep for interval
    sleep "$INTERVAL"
  done

}


# rebalanceStatus
# -----------------------------------
# Gets the rebalance status
# -----------------------------------
# shellcheck disable=SC2001
rebalanceStatus() {
  local cluster="${1}"
  debug "cluster: $cluster"
  local status
  # call the nodes endpoint
  local url="$PROTOCOL://$cluster:$PORT/pools/default/tasks"
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
    status=$(echo "$http_body" | jq --raw-output --compact-output \
      --argjson input "$http_body" \
      '[
        .[] |
        select(.type == "rebalance") | .status
        ][0]
      ')
  else
    error "Unable to reach the cluster: ${cluster} at ${url}"
    exit 1
  fi
  echo "$status"
}

# rebalance
# -----------------------------------
# Perform a rebalance
# -----------------------------------
# shellcheck disable=SC2001
rebalance() {
  local cluster="${1}"
  local otp_nodes="${2}"
  debug "cluster: $cluster"
  debug "otp_nodes: $otp_nodes"
  # call the nodes endpoint
  local url=$PROTOCOL://$cluster:$PORT/controller/rebalance
  debug "url: $url"
  info "Rebalancing Cluster: $cluster with nodes $otp_nodes"
  local http_response
  http_response=$(curl \
    --user "$USERNAME:$PASSWORD" \
    --silent \
    --data "knownNodes=$otp_nodes" \
    --connect-timeout "$TIMEOUT" \
    --write-out "HTTPSTATUS:%{http_code}" \
    "$url")

  debug " Response:   $http_response"
  local http_body
  http_body=$(echo "$http_response" | sed -e 's/HTTPSTATUS\:.*//g')
  local http_status
  http_status=$(echo "$http_response" | tr -d '\n' | sed -e 's/.*HTTPSTATUS://')
  # inspect the response code
  if [ "$http_status" -eq "200" ]; then
    echo "success"
  else
    error "Unable to reach the cluster: ${cluster} at ${url}"
    exit 1
  fi
}

# recoverNode
# -----------------------------------
# Recover a failed over node
# -----------------------------------
# shellcheck disable=SC2001
recoverNode() {
  local cluster="${1}"
  local otp_node="${2}"
  debug "cluster: $cluster"
  debug "otp_node: $otp_node"
  # call the nodes endpoint
  local url=$PROTOCOL://$cluster:$PORT/controller/setRecoveryType
  debug "url: $url"
  info "Recovering Node: $otp_node"
  local http_response
  http_response=$(curl \
    --user "$USERNAME:$PASSWORD" \
    --silent \
    --data "otpNode=$otp_node" \
    --data "recoveryType=full" \
    --connect-timeout "$TIMEOUT" \
    --write-out "HTTPSTATUS:%{http_code}" \
    "$url")

  debug " Response:   $http_response"
  local http_body
  http_body=$(echo "$http_response" | sed -e 's/HTTPSTATUS\:.*//g')
  local http_status
  http_status=$(echo "$http_response" | tr -d '\n' | sed -e 's/.*HTTPSTATUS://')
  # inspect the response code
  if [ "$http_status" -eq "200" ]; then
    echo "success"
  else
    error "Unable to reach the cluster: ${cluster} at ${url}"
    exit 1
  fi
}

# getOTPNodes
# -----------------------------------
# Gets a list of the OTP hostnames for rebalancing
# -----------------------------------
# shellcheck disable=SC2001
getOTPNodes() {
  local cluster="${1}"
  debug "cluster: $cluster"
  local nodes
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
    nodes=$(echo "$http_body" | jq --raw-output --compact-output \
      --argjson input "$http_body" \
      '[
        .nodes[] | .otpNode
        ] | join(",")
      ')
  else
    error "Unable to reach the cluster: ${cluster} at ${url}"
    exit 1
  fi
  echo "$nodes"
}

# failoverNode
# -----------------------------------
# Hard failover a node
# (Note: only nodes with the data service can be gracefully failed over)
# -----------------------------------
# shellcheck disable=SC2001
failoverNode() {
  local cluster="${1}"
  local otp_node="${2}"
  debug "cluster: $cluster"
  debug "otp_node: $otp_node"
  # call the nodes endpoint
  local url=$PROTOCOL://$cluster:$PORT/controller/failOver
  debug "url: $url"
  warning "Failing over Node: $otp_node"
  local http_response
  http_response=$(curl \
    --user "$USERNAME:$PASSWORD" \
    --silent \
    --data "otpNode=$otp_node" \
    --connect-timeout "$TIMEOUT" \
    --write-out "HTTPSTATUS:%{http_code}" \
    "$url")

  debug " Response:   $http_response"
  local http_body
  http_body=$(echo "$http_response" | sed -e 's/HTTPSTATUS\:.*//g')
  local http_status
  http_status=$(echo "$http_response" | tr -d '\n' | sed -e 's/.*HTTPSTATUS://')
  # inspect the response code
  if [ "$http_status" -eq "200" ]; then
    echo "success"
  else
    error "Unable to reach the cluster: ${cluster} at ${url}"
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
          {
            node: .hostname | split(":")[0],
            hostname: .hostname,
            mem_free: .systemStats.mem_free,
            mem_total: .systemStats.mem_total,
            percent_mem_free: (((.systemStats.mem_free / .systemStats.mem_total) * 100) | floor),
            percent_mem_used: (100 - ((.systemStats.mem_free / .systemStats.mem_total) * 100) | floor),
            status: .status,
            cluster_membership: .clusterMembership,
            recovery_type: .recoveryType,
            otp_node: .otpNode
          }
        ]
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
  local cluster_name
  # inspect the response code
  if [ "$http_status" -eq "200" ]; then
     # parse the response, append the indexes from the cluster to the global indexes variable
     cluster_name=$(echo "$http_body" | jq --raw-output --compact-output \
      '.clusterName')
  else
    cluster_name="N/A"
  fi
  # make sure there is a default name
  if [ -z "$cluster_name" ]; then
    cluster_name="Cluster Name Not Set"
  fi
  echo "$cluster_name"
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
