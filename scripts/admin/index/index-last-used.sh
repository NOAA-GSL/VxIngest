#!/usr/bin/env bash

version="1.1.0"

_usage() {
  echo -n "${__script} [OPTION]...

 Determines when each index in the cluster was last used.  The output can be to stdout
 or a CSV file.  The default behavior is to determine when every index in the cluster
 was last scanned, this can be filtered by bucket and/or indexes and/or replicas.
 Additionally, the precision can be determined by the standardized zoom stats parameter,
 the default is a year, for increased precision see the zoom parameter below.

 Options:
  -c, --cluster           The cluster address (default: localhost)  (default: localhost)
  -u, --username          Cluster Admin or RBAC username (default: Administrator)
  -p, --password          Cluster Admin or RBAC password (default: password)
  -b, --buckets           (optional) A comma-delimited list of buckets to filter on (default: *)
  -i, --indexes           (optional) A comma-delimited list of indexes to filter on (default: *)
  -e, --include-replicas  (optional) Whether or not to include the replicas in the output.  (default: false)
  -r, --port              (optional) The port to use (default: 8091)
  -l, --protocol          (optional) The protocol to use (default: http)
  -t, --timeout           (optional) The timeout to use for HTTP requests (default: 5)
  -z, --zoom              (optional) The option to sample: minute, hour, day, week, month, year (default: year)
  -o, --output            (optional) The output format to use, values can be console, stdout or csv (default: console)
  -d, --output-dir        (optional) The name of the output directory to use if output is csv (default: pwd)
  -f, --output-file       (optional) The name of the output file if output is csv (default: index-usage-yyyy-mm-ddThh:mm:ss)
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
BUCKETS=${BUCKETS:='*'}
INDEXES=${INDEXES:='*'}
INCLUDE_REPLICAS=${INCLUDE_REPLICAS:='true'}
ZOOM=${ZOOM:='year'}
OUTPUT=${OUTPUT:='console'}
OUTPUT_DIR=${OUTPUT_DIR:=$(pwd)}
OUTPUT_FILE="index-usage-$(date +"%Y-%m-%dT%H:%M:%S").csv"

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
      -b|--buckets) BUCKETS=${2} && shift 2;;
      -i|--indexes) INDEXES=${2} && shift 2;;
      -e|--include-replicas) INCLUDE_REPLICAS=${2} && shift 2;;
      -z|--zoom) ZOOM=${2} && shift 2;;
      -o|--output) OUTPUT=${2} && shift 2;;
      -d|--output-dir) OUTPUT_DIR=${2} && shift 2;;
      -f|--output-file) OUTPUT_FILE=${2} && shift 2;;
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
    warning "Do not specifiy the port for the hostname in the -c/--CLUSTER argument" && valid=false
  fi
  # validate the cluster argument does not contain the protocol
  if [[ "$CLUSTER" =~ https?:// ]]; then
    warning "Do not specifiy the protocol (http/https) for the hostnames in the -c/--CLUSTER argument" && valid=false
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
  # validate the include replicas argument is a boolean
  if ! [[ "$INCLUDE_REPLICAS" =~ ^(true|false)$ ]]; then
    warning "The -e/--include-replicas argument can only be \"true\" or \"false\"" && valid=false
  fi
  # validate the zoom argument minute, hour, day, week, month, year
  if ! [[ "$ZOOM" =~ ^(minute|hour|day|week|month|year)$ ]]; then
    warning "The -z/--zoom argument can only be: \"minute\", \"hour\", \"day\", \"week\", \"month\", \"year\"" && valid=false
  fi
  # validate the output argument is console or csv
  if ! [[ "$OUTPUT" =~ ^(console|csv)$ ]]; then
    warning "The -o/--output argument can only be: \"console\" or \"csv\"" && valid=false
  fi
  # validate the output directory exists
  if [ ! -d "$OUTPUT_DIR" ]; then
    warning "The output directory \"$OUTPUT_DIR\" for the -d/--output-dir argument does not exist" && valid=false
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
    include_replicas: $INCLUDE_REPLICAS
    zoom: $ZOOM
    output: $OUTPUT
    output_dir: $OUTPUT_DIR
    output_file: $OUTPUT_FILE
    port: $PORT
    protocol: $PROTOCOL
    timeout: $TIMEOUT"

    local indexes="" # local variable to hold all of the indexes
    # get all of the indexes in the cluster
    indexes=$(getClusterIndexes "$CLUSTER" "$BUCKETS" "$INDEXES" "$INCLUDE_REPLICAS")
    # variable to hold the last output bucket
    last_bucket=""

    # if the output format is csv, write the headings to a file
    if [[ "$OUTPUT" == "csv" ]]; then
      echo '"bucket","index","last_used","definition"' > "$OUTPUT_DIR/$OUTPUT_FILE"
      echo "" # add a blank line
    fi

    # loop over each of the indexes
    for row in $(echo "$indexes" | jq --raw-output '.[] | @base64'); do
      _jq() {
       echo "$row" | base64 --decode | jq -r "${1}"
      }
      # define local variables
      local result
      local bucket_name
      local index_name
      local index_definition
      # parse the individual row
      result=$(_jq '.')
      # get the bucket name
      bucket_name=$(echo "$result" | jq --raw-output '.bucket')
      # get the index name
      index_name=$(echo "$result" | jq --raw-output '.index')
      # get the index definition
      index_definition=$(echo "$result" | jq --raw-output '.definition')

      # if we're outputting to csv, output the name of the index we're working
      # on to the console so the user knows we're working on something
      if [[ "$OUTPUT" == "csv" ]]; then
        tput cuu1 && tput el # clear the previous line
        echo "Working on: $index_name"
      fi

      # get the last used
      local last_used;
      last_used=$(getIndexLastUsed "$CLUSTER" "$bucket_name" "$index_name")

      # if we're outputting to a csv file
      if [[ "$OUTPUT" == "csv" ]]; then
        jq --null-input --raw-output \
          --arg bucket "$bucket_name" \
          --arg index "$index_name" \
          --arg last_used "$last_used" \
          --arg definition "$index_definition" \
          '[ $bucket, $index, $last_used, $definition ] | @csv' >> "$OUTPUT_DIR/$OUTPUT_FILE"
      else
        # we're outputting to the console
        # output the bucket heading if it's different than the previous
        if [[ "$last_bucket" != "$bucket_name" ]]; then
          last_bucket="$bucket_name"
          echo ""
          echo "*******************************************************"
          echo "* Bucket: $bucket_name"
          echo "*******************************************************"
        fi
        # output the index and when it was last used
        echo "$index_name ($last_used)"
      fi
    done

    # if the output format is csv, write the headings to a file
    if [[ "$OUTPUT" == "csv" ]]; then
      tput cuu1 && tput el # clear the previous line
      echo "Results saved to: $OUTPUT_DIR/$OUTPUT_FILE"
    fi
}

# getIndexLastUsed
# -----------------------------------
# Retrieves when the index was last scanned via the REST APIs
# -----------------------------------
# shellcheck disable=SC2001
getIndexLastUsed() {
  local host="${1/:8091/}"
  local bucket="${2}"
  local index="${3}"
  debug "
  arguments:
    host: $host
    bucket: $bucket
    index: $index
  "
  # call the index status api
  local url
  url="$PROTOCOL://$host:$PORT/pools/default/buckets/$bucket/stats/index%2F$(urlEncode "$index")%2Fnum_requests?zoom=$ZOOM"
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
    echo "$http_body" | jq --raw-output \
      --arg host "$host" \
      --arg bucket "$bucket" \
      --arg index "$index" \
      '. as $stats |
      # set the timestamp array to a variable and reverse it
      $stats | (.timestamp | reverse) as $timestamps |
      # set the index node to a variable and reverse it
      # loop through each host
      $stats | reduce (.nodeStats | to_entries[]) as {$key, $value} ([];
        # reverse each of the hosts samples, loop through them save the first non-zero
        # value for each array
        . += [(reduce ($value | reverse | .[]) as $item ({ found: false, current: 0, pos: 0, timestamp: null, host: $key };
          # if the current item is not "undefined" or null
          # and the current item has a value greater than 0,
          if (.found == false and $item != "undefined" and $item != null and $item > 0) then
            .item = $item | .pos = .current | .found = true | .timestamp = $timestamps[.current]
          else
            .current = .current + 1
          end
        ))]
      ) |
      # filter out any items that were not found, sort by the timestamp asc then reverse the array
      [.[] | select(.found == true)] | sort_by(.timestamp) | reverse |
      if (. | length > 0) then
        (.[].timestamp / 1000 | todate)
      else
        "N/A"
      end
      '
  else
    error "Unable to reach the cluster: ${host} at ${url}"
    exit 1
  fi
}

# urlEncode
# -----------------------------------
# Utility function to url encode a string
# -----------------------------------
urlEncode() {
  # urlencode <string>
  local length="${#1}"
  for (( i = 0; i < length; i++ )); do
    local c="${1:i:1}"
    case $c in
      [a-zA-Z0-9.~_-]) printf "%s" "$c" ;;
      *) printf '%%%02X' "'$c"
    esac
  done
}

# getClusterIndexes
# -----------------------------------
# Retrieves the indexes for a cluster
# -----------------------------------
# shellcheck disable=SC2001
getClusterIndexes() {
  local cluster="${1}"
  local filter_buckets="${2}"
  local filter_indexes="${3}"
  local include_replicas="${4}"
  debug "
  arguments:
    cluster: $cluster
    filter_buckets: $filter_buckets
    filter_indexes: $filter_indexes
    include_replicas: $include_replicas
  "
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
      --arg include_replicas "$INCLUDE_REPLICAS" \
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
            ($include_replicas == "true" or ($include_replicas == "false" and ($current.index | contains("replica ") | not)))
          ) | {
            "index": $current.index,
            "bucket": $current.bucket,
            "host": $current.hosts[0],
            # strip off the WITH {...}
            "definition": ($current.definition | gsub(" WITH \\{.+$"; ""))
          }
        ] | sort_by(.bucket, .index)
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
