#!/bin/sh
if [[ $# -lt 1 || $# -gt 2 ]]; then
  echo "Usage $0 credentials-file"
  echo "This script assumes that you have cloned VXingest into ${HOME}/VXingest"
  echo "If you cloned it elsewhere, make a link"
  echo "This script assumes that you run it from ${HOME}VXingest/gsd_sql_to_cb"
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

# Do all the models using DO_MODEL
#HRRR
DO_MODEL "${HOME}/VxIngest/test/load_spec_gsd-hrrr-v01.yaml" "madis3.HRRR" "ceiling2.HRRR" "visibility.HRRR" "HRRR"

#HRRR_OPS
DO_MODEL "${HOME}/VxIngest/test/load_spec_gsd-hrrr_ops-v01.yaml" "madis3.HRRR_OPSqp" "ceiling2.HRRR_OPS" "visibility.HRRR_OPS" "HRRR_OPS"

#RAP_OPS - ceiling only
DO_MODEL "${HOME}/VxIngest/test/load_spec_gsd-rap_ops-v01.yaml" "madis3.RAP_OPSqp" "ceiling2.RAP_OPS" "none" "RAP_OPS"

#RRFS_dev1
DO_MODEL "${HOME}/VxIngest/test/load_spec_gsd-rrfs_dev1-v01.yaml" "madis3.RRFS_dev1qp" "ceiling2.RRFS_dev1" "RRFS_dev1" "RRFS_dev1"
