#!/bin/sh
# read and process a credentials file
function DO_CREDENTIALS() {
  credentials=$1
  m_host=$(grep mysql_host ${credentials} | awk '{print $2}')
  m_user=$(grep mysql_user ${credentials} | awk '{print $2}')
  m_password=$(grep mysql_password ${credentials} | awk '{print $2}')
  cb_host=$(grep cb_host ${credentials} | awk '{print $2}')
  cb_user=$(grep cb_user ${credentials} | awk '{print $2}')
  cb_pwd=$(grep cb_password ${credentials} | awk '{print $2}')
  cred="${cb_user}:${cb_pwd}"
}

# DO MODEL - this function is appropriate for all models
function DO_MODEL() {
  loadSpec=$1
  madisTableName=$2
  ceilingTableName=$3
  visibilityTableName=$4
  model=$5
  # find the max time in the gsd mysql database
  echo "In DO_MODEL"
  echo "query for max time: mysql -u${m_user} -p${m_password} -h${m_host} -B -N -e \"select max(time) from ${madisTableName};select max(time) from ${ceilingTableName};select max(time) from ${visibilityTableName};\""
  stop=$(mysql -u${m_user} -p${m_password} -h${m_host} -B -N -e "select max(time) from ${madisTableName}; \
  select max(time) from ${ceilingTableName}; \
  select max(time) from ${visibilityTableName};" |
    sort -n | head -1)
  # find the min time in the gsd mysql database - RAP_OPS doesn't have visibility
  echo "${visibilityTableName}" | grep -i none
  ret=$?
  if [[ ${ret} -eq 0 ]]; then
    echo "query for min ceiling time: "mysql -u${m_user} -p${m_password} -h${m_host} -B -N -e \"select min(time) from ${madisTableName};select min(time) from ${ceilingTableName};\""
    gsd_start=$(mysql -u${m_user} -p${m_password} -h${m_host} -B -N -e "select min(time) from ${madisTableName}; \
      select min(time) from ${ceilingTableName}; " |
      sort -n | tail -1)
  else
    echo "query for min ceiling time: mysql -u${m_user} -p${m_password} -h${m_host} -B -N -e \"select min(time) from ${madisTableName};select min(time) from ${ceilingTableName};select min(time) from ${visibilityTableName};\""
    gsd_start=$(mysql -u${m_user} -p${m_password} -h${m_host} -B -N -e "select min(time) from ${madisTableName};  \
      select min(time) from ${ceilingTableName};  \
      select min(time) from ${visibilityTableName};" |
      sort -n | tail -1)
  fi

  echo "find the max time in the couchbase"
  echo "curl -s -u ${cred} http://${cb_host}:8093/query/service -d \"statement=select max(mdata.fcstValidEpoch) as max_fcstValidEpoch from mdata WHERE type=\"DD\" and docType \"model\" and model= \"${model}\" and subset = \"METAR\" and version = \"V01\"\""
  cb_start=$(curl -s -u ${cred} http://${cb_host}:8093/query/service \
    -d "statement=select max(mdata.fcstValidEpoch) as max_fcstValidEpoch from mdata \
    WHERE type=\"DD\" and docType=\"model\" and model=\"${model}\" and subset=\"METAR\" and version=\"V01\"" | jq -r '.results | .[] | .max_fcstValidEpoch')
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
  export PYTHONPATH=${HOME}/VXingest
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
  region=$3
  # find the max time in the gsd mysql database
  stop=$(mysql -u${m_user} -p${m_password} -h${m_host} -B -N \
    -e "select max(time) from ${ctc_table_name};" | sort -n | head -1)
  # find the min time in the gsd mysql database
  gsd_start=$(mysql -u${m_user} -p${m_password} -h${m_host} -B -N \
    -e "select min(time) from ${ctc_table_name};" | sort -n | tail -1)
  # find the max time in the couchbase

  echo "curl -s -u ${cred} http://${cb_host}:8093/query/service -d \"statement=select max(mdata.fcstValidEpoch) as max_fcstValidEpoch from mdata " \
    "WHERE type=\\"DD\\" and docType = \\"CTC\\" and region=\\"${region}\\" and subset = \\"METAR\\" and version = \\"V01\\"\""
  cb_start=$(curl -s -u ${cred} http://${cb_host}:8093/query/service -d "statement=select max(mdata.fcstValidEpoch) as max_fcstValidEpoch from mdata WHERE type=\"DD\" and docType = \"CTC\" and region=\"${region}\" and subset = \"METAR\" and version = \"V01\"" | jq -r '.results | .[] | .max_fcstValidEpoch')
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
  export PYTHONPATH=${HOME}/VXingest
  while [[ $end -lt $stop ]]; do
    end=$(($cb_start + $week))
    echo "time python3 run_gsd_ingest_threads.py  -s ${loadSpec} -c ${credentials} -f $cb_start -l $end"
    time python3 run_gsd_ingest_threads.py -s ${loadSpec} -c ${credentials} -f $cb_start -l $end
    cb_start=$(($cb_start + $week))
  done
}

function DO_OBS_AND_STATIONS() {
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
  export PYTHONPATH=${HOME}/VXingest
  while [[ $end -lt $stop ]]; do
    end=$(($cb_start + $week))
    echo "Ingesting stations and obs from $cb_start through $end"
    # ingest the stations
    echo "time python3 run_gsd_ingest_threads.py  -s ${HOME}/VXingest/test/load_spec_gsd-stations-v01.yaml -c ${credentials} -f $cb_start -l $end"
    time python3 run_gsd_ingest_threads.py -s ${HOME}/VXingest/test/load_spec_gsd-stations-v01.yaml -c ${credentials} -f $cb_start -l $end
    # ingest the obs
    echo "time python3 run_gsd_ingest_threads.py  -s ${HOME}/VXingest/test/load_spec_gsd-metars-v01.yaml -c ${credentials} -f $cb_start -l $end"
    time python3 run_gsd_ingest_threads.py -s ${HOME}/VXingest/test/load_spec_gsd-metars-v01.yaml -c ${credentials} -f $cb_start -l $end
    cb_start=$(($cb_start + $week))
  done
}

