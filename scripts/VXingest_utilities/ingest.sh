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

DO_OBS_AND_STATIONS

# Do all the models using DO_MODEL
#HRRR
DO_MODEL "${HOME}/VXingest/test/load_spec_gsd-hrrr-v01.yaml" "madis3.HRRR" "ceiling2.HRRR" "visibility.HRRR" "HRRR"

#HRRR_OPS
DO_MODEL "${HOME}/VXingest/test/load_spec_gsd-hrrr_ops-v01.yaml" "madis3.HRRR_OPSqp" "ceiling2.HRRR_OPS" "visibility.HRRR_OPS" "HRRR_OPS"

#RAP_OPS - ceiling only
DO_MODEL "${HOME}/VXingest/test/load_spec_gsd-rap_ops-v01.yaml" "madis3.RAP_OPSqp" "ceiling2.RAP_OPS" "none" "RAP_OPS"

#RRFS_dev1
DO_MODEL "${HOME}/VXingest/test/load_spec_gsd-rrfs_dev1-v01.yaml" "madis3.RRFS_dev1qp" "ceiling2.RRFS_dev1" "RRFS_dev1" "RRFS_dev1"

#do all the contingency tables with DO_CTC
# CTC's are so similar that it is possible to use a function with 3 parameters to process them
DO_CTC "${HOME}/VXingest/test/load_spec_gsd-HRRR_ALL_HRRR_CTC-v01.yaml" \
  "ceiling_sums2.HRRR_ALL_HRRR" "HRRR_ALL_HRRR_CTC"

DO_CTC "${HOME}/VXingest/test/load_spec_gsd-HRRR_E_HRRR_CTC-v01.yaml" \
  "ceiling_sums2.HRRR_E_HRRR" "HRRR_E_HRRR_CTC"

DO_CTC "${HOME}/VXingest/test/load_spec_gsd-HRRR_E_US_CTC-v01.yaml" \
  "ceiling_sums2.HRRR_E_US" "HRRR_E_US_CTC"

DO_CTC "${HOME}/VXingest/test/load_spec_gsd-HRRR_Gtlk_CTC-v01.yaml" \
  "ceiling_sums2.HRRR_Gtlk" "HRRR_Gtlk_CTC"

DO_CTC "${HOME}/VXingest/test/load_spec_gsd-HRRR_OPS_ALL_HRRR_CTC-v01.yaml" \
  "ceiling_sums2.HRRR_OPS_ALL_HRRR" "HRRR_OPS_ALL_HRRR_CTC"

DO_CTC "${HOME}/VXingest/test/load_spec_gsd-HRRR_OPS_E_HRRR_CTC-v01.yaml" \
  "ceiling_sums2.HRRR_OPS_E_HRRR" "HRRR_OPS_E_HRRR_CTC"

DO_CTC "${HOME}/VXingest/test/load_spec_gsd-HRRR_OPS_E_US_CTC-v01.yaml" \
  "ceiling_sums2.HRRR_OPS_E_US" "HRRR_OPS_E_US_CTC"

DO_CTC "${HOME}/VXingest/test/load_spec_gsd-HRRR_OPS_Gtlk_CTC-v01.yaml" \
  "ceiling_sums2.HRRR_OPS_Gtlk" "HRRR_OPS_Gtlk_CTC"

DO_CTC "${HOME}/VXingest/test/load_spec_gsd-HRRR_OPS_W_HRRR_CTC-v01.yaml" \
  "ceiling_sums2.HRRR_OPS_W_HRRR" "HRRR_OPS_W_HRRR_CTC"

DO_CTC "${HOME}/VXingest/test/load_spec_gsd-HRRR_W_HRRR_CTC-v01.yaml" \
  "ceiling_sums2.HRRR_W_HRRR" "HRRR_W_HRRR_CTC"

DO_CTC "${HOME}/VXingest/test/load_spec_gsd-RRFS_dev1_ALL_HRRR_CTC-v01.yaml" \
  "ceiling_sums2.RRFS_dev1_ALL_HRRR" "RRFS_dev1_ALL_HRRR_CTC"

DO_CTC "${HOME}/VXingest/test/load_spec_gsd-RRFS_dev1_E_HRRR_CTC-v01.yaml" \
  "ceiling_sums2.RRFS_dev1_E_HRRR" "RRFS_dev1_E_HRRR_CTC"

DO_CTC "${HOME}/VXingest/test/load_spec_gsd-RRFS_dev1_E_US_CTC-v01.yaml" \
  "ceiling_sums2.RRFS_dev1_E_US" "RRFS_dev1_E_US_CTC"

DO_CTC "${HOME}/VXingest/test/load_spec_gsd-RRFS_dev1_Gtlk_CTC-v01.yaml" \
  "ceiling_sums2.RRFS_dev1_Gtlk" "RRFS_dev1_Gtlk_CTC"

DO_CTC "${HOME}/VXingest/test/load_spec_gsd-RRFS_dev1_W_HRRR_CTC-v01.yaml" \
  "ceiling_sums2.RRFS_dev1_W_HRRR" "RRFS_dev1_W_HRRR_CTC"
