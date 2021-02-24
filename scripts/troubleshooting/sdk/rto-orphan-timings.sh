#!/usr/bin/env bash

# set the defaults, these can all be overriden as environment variables or passed via the cli
SCRIPT=$(basename "$0")
LOG_DIR=${LOG_DIR:=$(pwd)}
OUTPUT_FILE="rto-orphan-stats-$(date +"%Y-%m-%dT%H:%M:%S").csv"
OUTPUT_DIR=""

# colors
RED=$(tput setaf 1)
GREEN=$(tput setaf 2)
YELLOW=$(tput setaf 3)
NORMAL=$(tput sgr0)

# ************************************************************************************
# Function: _usage
#   Output script usage information
# ************************************************************************************
function _usage
{
  echo "Description:"
  echo "  This script will take a directory of one or more log files and parse them for the RTO Orphan information"
  echo " "
  echo "Usage: ./$SCRIPT [options]"
  echo " "
  echo "Options:"
  echo "  --logs=<s>          The log directory path"
  echo "  --outputFile=<s>    The name of the output file"
  echo "  --outputDir=<s>     The output directory path"
  echo " "
  echo "Example:"
  echo "  ./$SCRIPT --logs=/mylogs"
  exit 5
}

# ************************************************************************************
#  Function: _get_logs
#   Get all of the log files from the directory
# ************************************************************************************
function _get_logs
{
  local start_time
  start_time=$(date +%s%N) # start of execution time
  # make sure the directory exists, if it doesn't error and exit
  if [ -d "$1" ]; then
    echo "Searching $1 for collect infos"
  else
    echo "${RED}Error: Could not find folder${NORMAL}"
    echo ""
    _usage
    exit 1
  fi

  log_files=$(ls -I "*.csv" "$1") # get a list of all of the log files
  log_files_count=$(echo "$log_files" | wc -w) # get a total count of the number of indexes in the bucket
  counter=0 # counter to keep track of which file we're working on

  # loop over each of the log files
  # shellcheck disable=SC2068
  for log_file in ${log_files[@]}
  do
    # increment the counter
    ((counter++))

    # move the output back a line, erase it and output the current file being worked on
    # this is just to keep the shell output cleaner and it's cool ¯\_(ツ)_/¯
    tput cuu 1 && tput el && echo "Working on file ($counter/$log_files_count): ${log_file}"

    # parse the log file
    result=$(_parse_log "$1/$log_file")

    # if we have already processed the first file, strip the column headings from the csv output
    if [[ $counter -ne 1 ]]; then
      result=$(echo "$result" | tail -n +2)
    fi

    # append the results to the output file if there is something to output
    if [ -n "$result" ]; then
      echo "$result" >> "$OUTPUT_DIR/$OUTPUT_FILE"
    fi
  done

  # calculate execution times and finalize output
  local end_time
  end_time=$(date +%s%N)
  local exec_time
  exec_time=$(((end_time - start_time) / 1000000))
  tput cuu 1 && tput el && echo "${GREEN}Processed $counter log files in ${exec_time}ms${NORMAL}"
  echo "Output: $OUTPUT_DIR/$OUTPUT_FILE"
}

# ************************************************************************************
#  Function: _parse_log
#   Parse the log file into a csv string and output it to a file
# ************************************************************************************
function _parse_log
{
  # find each of the RTO entries in the log files, strip the JSON and create a single
  # array of the entries to loop over and parse.  If you want to see the raw entries,
  # run: cat application.log | grep "Operations over threshold:"
  local rto_entries
  rto_entries="[$(
  # if it is a zip file
  if [[ $1 == *.zip ]] ; then
    unzip -p "$1"
  else # otherwise assume it is a log file already
    cat "$1"
  fi | \
     # find the rto entries
    grep "Orphan responses observed:" | \
     # remove everything from the start of the line until the rto json
    sed 's/.*: //' | \
    # output each entry to a single line
    tr '\n' ',' | \
    # remove the last character: ,
    sed 's/,$//'
  )]"

  # check and see if we have any entries to work with
  if [ "$rto_entries" != "[]" ]; then
    # pass the raw json string to jq so it can be parsed, since there could be more
    # than 1 log file the name of the log file is passed so it can be determined
    # where the entry came from
    #
    # Note: that the Orphan entries in the log were inadvertently left minified as of
    # SDK 2.7.5, these values are mapped as follows:
    #   - b = bucket
    #   - l = last_local_address
    #   - c = last_local_id
    #   - i = last_operation_id
    #   - r = last_remote_address
    #   - d = server_us
    #
    # The output below uses the jq alternative operator (//) first referencing the
    # proper value, then defaulting back to the minified value, once the SDK is updated
    # the script will continue to work as expected
    echo "$rto_entries" | jq -r --arg log_file "$(basename "$1")" '
    def tocsv($x):
      $x
      | (map(keys)
          | add
          | unique
          | sort
      ) as $cols
      | map(. as $row
          | $cols
          | map($row[.] | tostring)
      ) as $rows
      | $cols, $rows[]
      | @csv;
    [
      .[][] | .service as $service | .top[] |
      {
        "bucket": (.bucket // .b),
        "last_local_address": (.last_local_address // .l),
        "last_local_id": (.last_local_id // .c),
        "last_operation_id": (.last_operation_id // .i),
        "last_remote_address": (.last_remote_address // .r),
        "log_file": $log_file,
        "server_us": (.server_us // .d),
        "service": $service
      }
    ] | tocsv(.)'
  fi
}

# parse any cli arguments
while [ $# -gt 0 ]; do
  case "$1" in
    --logs=*)
      LOG_DIR="${1#*=}"
      ;;
    --outputFile=*)
      OUTPUT_FILE="${1#*=}"
      ;;
    --outputDir=*)
      OUTPUT_DIR="${1#*=}"
      ;;
    --help )
      _usage
      ;;
    *)
      echo "${RED}ERROR : Invalid command line option: $1 ${NORMAL}"
      _usage
      ;;
  esac
  shift
done

# if no output directory is defined, use the log directory
OUTPUT_DIR=${OUTPUT_DIR:=$LOG_DIR}

# make sure jq exists
if [ "$(command -v jq)" = "" ]; then
  echo >&2 "jq command is required, see (https://stedolan.github.io/jq/download)";
  exit 1;
fi

# If the output file exists remove it
if [ -f "$OUTPUT_DIR/$OUTPUT_FILE" ]; then
  echo "${YELLOW}The output file $OUTPUT_DIR/$OUTPUT_FILE exists and will be removed${NORMAL}"
  rm -rf "${OUTPUT_DIR:?}/${OUTPUT_FILE:?}"
fi

# start by getting the logs
_get_logs "$LOG_DIR"
