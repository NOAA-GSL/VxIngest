#!/bin/sh
if [[ $# -lt 1 || $# -gt 2 ]]; then
  echo "Usage $0 credentials-file"
  echo "This script assumes that you have cloned VXingest into ${HOME}/VXingest"
  echo "If you cloned it elsewhere, make a link"
  echo "This script assumes that you run it from ${HOME}VXingest/classic_sql_to_cb"
  exit 1
fi
  force_stop=$(date +%s)
  if [[ $# -gt 1 ]]; then
    force_stop=$2
  fi
  credentials=$1


# source the functions file
. ${HOME}/VXingest/scripts/VXingest_utilities/ingest_functions.sh

DO_CREDENTIALS "$credentials"
DO_OBS_AND_STATIONS
