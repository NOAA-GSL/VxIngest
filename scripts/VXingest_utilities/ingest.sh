#!/bin/sh
if [[ $# -lt 1 || $# -gt 2 ]]; then
  echo "Usage $0 credentials-file"
  echo "This script assumes that you have cloned VXingest into ${HOME}/VXingest"
  echo "If you cloned it elsewhere, make a link"
  echo "This script assumes that you run it from ${HOME}VXingest/gsd_sql_to_cb"
  exit 1
fi
credentials=$1
force_stop=$(date +%s)
if [[ $# -gt 1 ]]; then
  force_stop=$2
fi
m_host=$(grep mysql_host ${credentials} | awk '{print $2}')
m_user=$(grep mysql_user ${credentials} | awk '{print $2}')
m_password=$(grep mysql_password ${credentials} | awk '{print $2}')
cb_host=$(grep cb_host ${credentials} | awk '{print $2}')
cb_user=$(grep cb_user ${credentials} | awk '{print $2}')
cb_pwd=$(grep cb_password ${credentials} | awk '{print $2}')
cred="${cb_user}:${cb_pwd}"
# DO MODEL - this function is appropriate for all models
function DO_MODEL() {
  loadSpec=$1
  madisTableName=$2
  ceilingTableName=$3
  visibilityTableName=$4
  docType=$5
  # find the max time in the gsd mysql database
  stop=$(mysql -u${m_user} -p${m_password} -h${m_host} -B -N -e "select max(time) from ${madisTableName};" \
    "select max(time) from ${ceilingTableName};" \
    "select max(time) from ${visibilityTableName};" \
    | sort -n | head -1)
  # find the min time in the gsd mysql database - RAP_OPS doesn't have visibility
  echo "${visibilityTableName}" | grep -i none
  ret=$?
  if [[ ${ret} -eq 0 ]]; then
    gsd_start=$(mysql -u${m_user} -p${m_password} -h${m_host} -B -N -e "select min(time) from ${madisTableName}; " \
      "select min(time) from ${ceilingTableName}; " \
      | sort -n | tail -1)
  else
    gsd_start=$(mysql -u${m_user} -p${m_password} -h${m_host} -B -N -e "select min(time) from ${madisTableName}; " \
      "select min(time) from ${ceilingTableName}; " \
      "select min(time) from ${visibilityTableName};" \
      | sort -n | tail -1)
  fi
  # find the max time in the couchbase
  echo "curl -s -u ${cred} http://${cb_host}:8093/query/service -d \"statement=select max(mdata.fcstValidEpoch) as max_fcstValidEpoch from mdata WHERE type=\"DD\" and docType = \"${docType}\" and subset = \"METAR\" and version = \"V01\"\""
  cb_start=$(curl -s -u ${cred} http://${cb_host}:8093/query/service \
    -d "statement=select max(mdata.fcstValidEpoch) as max_fcstValidEpoch from mdata \
    WHERE type=\"DD\" and docType=\"${docType}\" and subset=\"METAR\" and version=\"V01\"" | jq -r '.results | .[] | .max_fcstValidEpoch')
  echo gsd_start is ${gsd_start} cb_start is ${cb_start}
  if [[ $cb_start == "null" ]]; then
    echo Using minimum time from gsd mysql database
    cb_start=${gsd_start}
  fi
  echo start is $cb_start
  echo stop is $stop
  # do one week at a time to make it easier on the gsd database
  week=604800
  end=$cb_start
  export PYTHONPATH=${HOME}/VxIngest
  while [[ $end -lt $stop ]]; do
    end=$(($cb_start + $week))
    echo "time python3 run_gsd_ingest_threads.py -s ${loadSpec} -c ${credentials} -f $cb_start -l $end"
    time python3 run_gsd_ingest_threads.py -s ${loadSpec} -c ${credentials} -f $cb_start -l $end
    cb_start=$(($cb_start + $week))
  done
}

function DO_CTC() {
  loadSpec=$1
  ctc_table_name=$2
  docType=$3
  # find the max time in the gsd mysql database
  stop=$(mysql -u${m_user} -p${m_password} -h${m_host} -B -N \
    -e "select max(time) from ${ctc_table_name};" | sort -n | head -1)
  # find the min time in the gsd mysql database
  gsd_start=$(mysql -u${m_user} -p${m_password} -h${m_host} -B -N \
    -e "select min(time) from ${ctc_table_name};" | sort -n | tail -1)
  # find the max time in the couchbase

  echo "curl -s -u ${cred} http://${cb_host}:8093/query/service -d \"statement=select max(mdata.fcstValidEpoch) as max_fcstValidEpoch from mdata " \
    "WHERE type=\"DD\" and docType = \"${docType}\" and subset = \"METAR\" and version = \"V01\"\""
  cb_start=$(curl -s -u ${cred} http://${cb_host}:8093/query/service \
    -d "statement=select max(mdata.fcstValidEpoch) as max_fcstValidEpoch from mdata \
    WHERE type=\"DD\" and docType = \"${docType}\" and subset = \"METAR\" and version = \"V01\"" | jq -r '.results | .[] | .max_fcstValidEpoch')
  echo gsd_start is ${gsd_start} cb_start is ${cb_start}
  if [[ $cb_start == "null" ]]; then
    echo Using minimum time from gsd mysql database
    cb_start=${gsd_start}
  fi
  echo start is $cb_start
  echo stop is $stop
  # do one week at a time to make it easier on the gsd database
  week=604800
  end=$cb_start
  export PYTHONPATH=${HOME}/VxIngest
  while [[ $end -lt $stop ]]; do
    end=$(($cb_start + $week))
    echo "time python3 run_gsd_ingest_threads.py  -s ${loadSpec} -c ${credentials} -f $cb_start -l $end"
    time python3 run_gsd_ingest_threads.py -s ${loadSpec} -c ${credentials} -f $cb_start -l $end
    cb_start=$(($cb_start + $week))
  done
}

# for obs and stations we do them in this specific routine
# find the max time in the gsd mysql database
stop=$(mysql -u${m_user} -p${m_password} -h${m_host} -B -N -e "select max(time) from madis3.obs; select max(time) from ceiling2.obs; select max(time) from visibility.obs;" | sort -n | head -1)
# find the min time in the gsd mysql database
gsd_start=$(mysql -u${m_user} -p${m_password} -h${m_host} -B -N -e "select min(time) from madis3.obs; select min(time) from ceiling2.obs; select min(time) from visibility.obs;" | sort -n | tail -1)
# find the max time in the couchbase
echo "curl -s -u ${cred} http://${cb_host}:8093/query/service -d 'statement=select max(mdata.fcstValidEpoch) as max_fcstValidEpoch from mdata WHERE type=\"DD\" and docType=\"obs\" and subset=\"METAR\" and version=\"V01\"' | jq -r '.results | .[] | .max_fcstValidEpoch'"
cb_start=$(curl -s -u ${cred} http://${cb_host}:8093/query/service -d 'statement=select max(mdata.fcstValidEpoch) as max_fcstValidEpoch from mdata WHERE type="DD" and docType="obs" and subset="METAR" and version="V01"' | jq -r '.results | .[] | .max_fcstValidEpoch')
echo gsd_start is ${gsd_start} cb_start is ${cb_start}
if [[ $cb_start == "null" ]]; then
  echo Using minimum time from gsd mysql database
  cb_start=${gsd_start}
fi
echo start is $cb_start
echo stop is $stop
# do one week at a time to make it easier on the gsd database
week=604800
end=$cb_start
export PYTHONPATH=${HOME}/VxIngest
while [[ $end -lt $stop ]]; do
  end=$(($cb_start + $week))
  echo "Ingesting stations and obs from $cb_start through $end"
  # ingest the stations
  echo "time python3 run_gsd_ingest_threads.py  -s ${HOME}/VxIngest/test/load_spec_gsd-stations-v01.yaml -c ${credentials} -f $cb_start -l $end"
  time python3 run_gsd_ingest_threads.py -s ${HOME}/VxIngest/test/load_spec_gsd-stations-v01.yaml -c ${credentials} -f $cb_start -l $end
  # ingest the obs
  echo "time python3 run_gsd_ingest_threads.py  -s ${HOME}/VxIngest/test/load_spec_gsd-metars-v01.yaml -c ${credentials} -f $cb_start -l $end"
  time python3 run_gsd_ingest_threads.py -s ${HOME}/VxIngest/test/load_spec_gsd-metars-v01.yaml -c ${credentials} -f $cb_start -l $end
  cb_start=$(($cb_start + $week))
done

# Do all the models using DO_MODEL
#HRRR
DO_MODEL "${HOME}/VxIngest/test/load_spec_gsd-hrrr-v01.yaml" "madis3.HRRR" "ceiling2.HRRR" "visibility.HRRR" "HRRR"

#HRRR_OPS
DO_MODEL "${HOME}/VxIngest/test/load_spec_gsd-hrrr_ops-v01.yaml" "madis3.HRRR_OPSqp" "ceiling2.HRRR_OPS" "visibility.HRRR_OPS" "HRRR_OPS"

#RAP_OPS - ceiling only
DO_MODEL "${HOME}/VxIngest/test/load_spec_gsd-rap_ops-v01.yaml" "madis3.RAP_OPSqp" "ceiling2.RAP_OPS" "none" "RAP_OPS"

#RRFS_dev1
DO_MODEL "${HOME}/VxIngest/test/load_spec_gsd-rrfs_dev1-v01.yaml" "madis3.RRFS_dev1qp" "ceiling2.RRFS_dev1" "RRFS_dev1" "RRFS_dev1"

#do all the contingency tables with DO_CTC
# CTC's are so similar that it is possible to use a function with 3 parameters to process them
DO_CTC "${HOME}/VxIngest/test/load_spec_gsd-HRRR_ALL_HRRR_CTC-v01.yaml" \
  "ceiling_sums2.HRRR_ALL_HRRR" "HRRR_ALL_HRRR_CTC"

DO_CTC "${HOME}/VxIngest/test/load_spec_gsd-HRRR_E_HRRR_CTC-v01.yaml" \
  "ceiling_sums2.HRRR_E_HRRR" "HRRR_E_HRRR_CTC"

DO_CTC "${HOME}/VxIngest/test/load_spec_gsd-HRRR_E_US_CTC-v01.yaml" \
  "ceiling_sums2.HRRR_E_US" "HRRR_E_US_CTC"

DO_CTC "${HOME}/VxIngest/test/load_spec_gsd-HRRR_Gtlk_CTC-v01.yaml" \
  "ceiling_sums2.HRRR_Gtlk" "HRRR_Gtlk_CTC"

DO_CTC "${HOME}/VxIngest/test/load_spec_gsd-HRRR_OPS_ALL_HRRR_CTC-v01.yaml" \
  "ceiling_sums2.HRRR_OPS_ALL_HRRR" "HRRR_OPS_ALL_HRRR_CTC"

DO_CTC "${HOME}/VxIngest/test/load_spec_gsd-HRRR_OPS_E_HRRR_CTC-v01.yaml" \
  "ceiling_sums2.HRRR_OPS_E_HRRR" "HRRR_OPS_E_HRRR_CTC"

DO_CTC "${HOME}/VxIngest/test/load_spec_gsd-HRRR_OPS_E_US_CTC-v01.yaml" \
  "ceiling_sums2.HRRR_OPS_E_US" "HRRR_OPS_E_US_CTC"

DO_CTC "${HOME}/VxIngest/test/load_spec_gsd-HRRR_OPS_Gtlk_CTC-v01.yaml" \
  "ceiling_sums2.HRRR_OPS_Gtlk" "HRRR_OPS_Gtlk_CTC"

DO_CTC "${HOME}/VxIngest/test/load_spec_gsd-HRRR_OPS_W_HRRR_CTC-v01.yaml" \
  "ceiling_sums2.HRRR_OPS_W_HRRR" "HRRR_OPS_W_HRRR_CTC"

DO_CTC "${HOME}/VxIngest/test/load_spec_gsd-HRRR_W_HRRR_CTC-v01.yaml" \
  "ceiling_sums2.HRRR_W_HRRR" "HRRR_W_HRRR_CTC"

DO_CTC "${HOME}/VxIngest/test/load_spec_gsd-RRFS_dev1_ALL_HRRR_CTC-v01.yaml" \
  "ceiling_sums2.RRFS_dev1_ALL_HRRR" "RRFS_dev1_ALL_HRRR_CTC"

DO_CTC "${HOME}/VxIngest/test/load_spec_gsd-RRFS_dev1_E_HRRR_CTC-v01.yaml" \
  "ceiling_sums2.RRFS_dev1_E_HRRR" "RRFS_dev1_E_HRRR_CTC"

DO_CTC "${HOME}/VxIngest/test/load_spec_gsd-RRFS_dev1_E_US_CTC-v01.yaml" \
  "ceiling_sums2.RRFS_dev1_E_US" "RRFS_dev1_E_US_CTC"

DO_CTC "${HOME}/VxIngest/test/load_spec_gsd-RRFS_dev1_Gtlk_CTC-v01.yaml" \
  "ceiling_sums2.RRFS_dev1_Gtlk" "RRFS_dev1_Gtlk_CTC"

DO_CTC "${HOME}/VxIngest/test/load_spec_gsd-RRFS_dev1_W_HRRR_CTC-v01.yaml" \
  "ceiling_sums2.RRFS_dev1_W_HRRR" "RRFS_dev1_W_HRRR_CTC"
