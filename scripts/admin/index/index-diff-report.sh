#!/usr/bin/env bash

version="1.0.0"

_usage() {
  echo -n "${__script} [OPTION]...

 Compares the indexes names and definitions across two or more clusters, optionally filtering on buckets

 Options:
  -c, --clusters          A comma delimited list of clusters
  -u, --username          Cluster Admin or RBAC username (default: Administrator)
  -p, --password          Cluster Admin or RBAC password (default: password)
  -b, --buckets           A comma-delimited list of buckets to filter on (default:*)
  -i, --include-replicas  Whether or not to include replica indexes in the results (default:true)
  -r, --port              The port to use (default: 8093)
  -s, --protocol          The protocol to use (default: http)
  -t, --timeout           The timeout to use for HTTP requests (default: 5)
  --log-level             The log level to to use 0-7 (default: 6)
  --debug                 Shortcut for --log-level 7
  --help                  Display this help and exit
  --version               Output version information and exit
"
}

# default variables / flags and their optional corresponding environment variables used in the script
CLUSTERS=${CLUSTER:='localhost'}
USERNAME=${CB_USERNAME:='Administrator'}
PASSWORD=${CB_PASSWORD:='password'}
BUCKETS=${BUCKETS:='*'}
INCLUDE_REPLICAS=${INCLUDE_REPLICAS:='true'}
PORT=${PORT:='8091'}
PROTOCOL=${PROTOCOL:='http'}
TIMEOUT=${TIMEOUT:=5}

# _options
# -----------------------------------
# Parses CLI options
# -----------------------------------
_options() {
  debug ""
  # Read the options and set stuff
  while [[ $# -gt 0 ]]; do
    case "$1" in
      -c|--clusters) CLUSTERS=${2} && shift 2;;
      -b|--buckets) BUCKETS=${2} && shift 2;;
      -i|--include-replicas) INCLUDE_REPLICAS=${2} && shift 2;;
      -r|--port) PORT=${2} && shift 2;;
      -s|--protocol) PROTOCOL=${2} && shift 2;;
      -t|--timeout) TIMEOUT=${2} && shift 2;;
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
  # validate the clusters argument does not contain any port references
  if [[ "$CLUSTERS" =~ :[0-9]+ ]]; then
    warning "Do not specifiy the port for the hostnames in the -c/--clusters argument" && valid=false
  fi
  # validate the clusters argument does not contain the protocol
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
  # validate the include replicas argument is a boolean
  if ! [[ "$INCLUDE_REPLICAS" =~ ^(true|false)$ ]]; then
    warning "The -i/--include-replicas argument can only be \"true\" or \"false\"" && valid=false
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
    buckets: $BUCKETS
    include-replicas: $INCLUDE_REPLICAS
    username: $USERNAME
    password: ********
    port: $PORT
    protocol: $PROTOCOL
    timeout: $TIMEOUT"

  local indexes="" # local variable to hold all of the indexes
  # loop over each of the clusters and get all of the indexes
  for cluster in $(echo "$CLUSTERS" | jq --slurp --raw-output --raw-input 'split(",") | .[]')
  do
    # get the indexes for the cluster
    indexes+="$(getClusterIndexes "$cluster")\n"
  done
  # combine the indexes from all of the clusters to a single json object
  indexes=$(echo -e "$indexes" | jq --slurp --raw-output --compact-output 'add')

  local names="" # local variable to hold all of the names
  # loop over each of the clusters and get all of the cluster names
  for cluster in $(echo "$CLUSTERS" | jq --slurp --raw-output --raw-input 'split(",") | .[]')
  do
    # get the indexes for the cluster
    names+="[{\"cluster\": \"$cluster\", \"clusterName\": \"$(getClusterName "$cluster")\"}]\n"
  done
  # combine the indexes from all of the clusters to a single json object where the key is the cluster hostname and the value is the name
  names=$(echo -e "$names" | jq --slurp --raw-output --compact-output 'add | reduce .[] as $item ({}; .[$item.cluster] = $item.clusterName)')

  echo "$indexes" | jq --raw-output --arg clusters "$CLUSTERS" --argjson names "$names" '
    . | sort_by(.bucket, .index) as $indexes |
    # convert clusters to an array that can be iterated over
    $clusters | split(",") as $clusters |
    # loop over each of the clusters and find the indexes in that cluster that are not in the others
    $clusters | .[] as $cluster |
      # build an array of objects with the index details in just the current cluster
      [ $indexes[] | select(.cluster == $cluster) ] as $current_cluster |
      # build a dictionary lookup for the definitions
      (reduce $current_cluster[] as $item ({};
        if (.[$item.bucket] == null) then
          .[$item.bucket] = {}
        else
          .
        end |
        .[$item.bucket][$item.index] = $item.definition
      )) as $current_cluster_lookup |
      # build an array of objects with the index details from all of the other clusters except the current
      [ $indexes[] | select(.cluster != $cluster) ] as $other_clusters |
      "Cluster: " + $names[$cluster] + " - " + $cluster + "\n" +
      "----------------------------------------------------------\n" + (
        # loop through the other clusters to see if the current cluster contains the index name and definition
        reduce $other_clusters[] as $item ("";
          if ($current_cluster_lookup[$item.bucket][$item.index] == null) then
            . + "- Missing Index: `" + $item.bucket + "`.`" + $item.index + "` from cluster \"" + $item.cluster + "\"\n"
          elif ($item.definition != $current_cluster_lookup[$item.bucket][$item.index]) then
            . + "- Mismatch Definition: `" + $item.bucket + "`.`" + $item.index + "` is different from cluster \"" + $item.cluster + "\"\n"
          else
            .
          end
        )
      ) + "\n"
  '
}

# getClusterIndexes
# -----------------------------------
# Retrieves the indexes for a cluster
# -----------------------------------
# shellcheck disable=SC2001
getClusterIndexes() {
  local cluster="${1}"
  debug "cluster: $cluster"
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
      --arg buckets "$BUCKETS" \
      --arg include_replicas "$INCLUDE_REPLICAS" \
      --arg cluster "$cluster" \
      '.indexes as $indexes |
      $buckets | gsub("\\*"; "") | split(",") as $buckets |
      $indexes |
        [ .[] | . as $current |
          select(
            (($buckets | length == 0) or (reduce $buckets[] as $item (false; if (. == false and $item == $current.bucket) then . = true else . end)))
            and
            ($include_replicas == "true" or ($include_replicas == "false" and ($current.index | contains("replica ") | not)))
          ) | {
            "cluster": $cluster,
            "index": $current.index,
            "bucket": $current.bucket,
            # strip off the WITH {...}
            "definition": ($current.definition | gsub(" WITH \\{.+$"; ""))
          }
        ] | unique
      '

      exit
  else
    error "Unable to reach the cluster: ${cluster} at ${url}"
    exit 1
  fi
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
     echo "$http_body" | jq --raw-output --compact-output \
      '.clusterName'
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
