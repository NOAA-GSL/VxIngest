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

#do all the contingency tables with DO_CTC
# CTC's are so similar that it is possible to use a function with 3 parameters to process them
DO_CTC "${HOME}/VXingest/test/load_spec_gsd-HRRR_ALL_HRRR_CTC-v01.yaml" \
  "ceiling_sums2.HRRR_ALL_HRRR" "ALL_HRRR"

DO_CTC "${HOME}/VXingest/test/load_spec_gsd-HRRR_E_HRRR_CTC-v01.yaml" \
  "ceiling_sums2.HRRR_E_HRRR" "E_HRRR"

DO_CTC "${HOME}/VXingest/test/load_spec_gsd-HRRR_W_HRRR_CTC-v01.yaml" \
  "ceiling_sums2.HRRR_W_HRRR" "W_HRRR"

DO_CTC "${HOME}/VXingest/test/load_spec_gsd-HRRR_E_US_CTC-v01.yaml" \
  "ceiling_sums2.HRRR_E_US" "E_US"

DO_CTC "${HOME}/VXingest/test/load_spec_gsd-HRRR_Gtlk_CTC-v01.yaml" \
  "ceiling_sums2.HRRR_Gtlk" "Gtlk"

DO_CTC "${HOME}/VXingest/test/load_spec_gsd-HRRR_OPS_ALL_HRRR_CTC-v01.yaml" \
  "ceiling_sums2.HRRR_OPS_ALL_HRRR" "ALL_HRRR"

DO_CTC "${HOME}/VXingest/test/load_spec_gsd-HRRR_OPS_E_HRRR_CTC-v01.yaml" \
  "ceiling_sums2.HRRR_OPS_E_HRRR" "E_HRRR"

DO_CTC "${HOME}/VXingest/test/load_spec_gsd-HRRR_OPS_W_HRRR_CTC-v01.yaml" \
  "ceiling_sums2.HRRR_OPS_W_HRRR" "W_HRRR"

DO_CTC "${HOME}/VXingest/test/load_spec_gsd-HRRR_OPS_E_US_CTC-v01.yaml" \
  "ceiling_sums2.HRRR_OPS_E_US" "E_US"

DO_CTC "${HOME}/VXingest/test/load_spec_gsd-HRRR_OPS_Gtlk_CTC-v01.yaml" \
  "ceiling_sums2.HRRR_OPS_Gtlk" "Gtlk"

DO_CTC "${HOME}/VXingest/test/load_spec_gsd-RRFS_dev1_ALL_HRRR_CTC-v01.yaml" \
  "ceiling_sums2.RRFS_dev1_ALL_HRRR" "ALL_HRRR"

DO_CTC "${HOME}/VXingest/test/load_spec_gsd-RRFS_dev1_E_HRRR_CTC-v01.yaml" \
  "ceiling_sums2.RRFS_dev1_E_HRRR" "E_HRRR"

DO_CTC "${HOME}/VXingest/test/load_spec_gsd-RRFS_dev1_W_HRRR_CTC-v01.yaml" \
  "ceiling_sums2.RRFS_dev1_W_HRRR" "W_HRRR"

DO_CTC "${HOME}/VXingest/test/load_spec_gsd-RRFS_dev1_E_US_CTC-v01.yaml" \
  "ceiling_sums2.RRFS_dev1_E_US" "E_US"

DO_CTC "${HOME}/VXingest/test/load_spec_gsd-RRFS_dev1_Gtlk_CTC-v01.yaml" \
  "ceiling_sums2.RRFS_dev1_Gtlk" "Gtlk"

