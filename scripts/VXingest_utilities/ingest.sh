#!/bin/sh
if [ $# -ne 1 ]; then
  echo "Usage $0 credentials-file"
  echo "This script assumes that you have cloned VXingest into ${HOME}/VXingest"
  echo "If you cloned it elsewhere, make a link"
  echo "This script assumes that you run it from ${HOME}VXingest/gsd_sql_to_cb"
  exit 1
fi
credentials=$1
# find the max obs time in the couchbase
start=$(${HOME}/VxIngest/scripts/VXingest_utilities/find_cb_max_obs_epoch.sh ${credentials})
if [ $start == "null" ]; then
	# find the max obs time in the gsd mysql database
	echo Using minimum time from gsd mysql database
	start=$(${HOME}/VxIngest/scripts/VXingest_utilities/find_gsd_min_obs_epoch.sh ${credentials})
fi
echo start is $start
# find the max obs time in the gsd mysql database
stop=$(${HOME}/VxIngest/scripts/VXingest_utilities/find_gsd_max_obs_epoch.sh ${credentials})
echo stop is $stop
week=604800
end=$((start+week))
export PYTHONPATH=${HOME}/VxIngest
while [ $end -lt $stop ]; do
	echo "$start   $end"
	start=$(( $start + $week ))
	end=$(( $end +$week ))

	echo "time python3 run_gsd_ingest_threads.py  -s ${HOME}/VxIngest/test/load_spec_gsd-stations-v01.yaml -c ${credentials} -f $start -l $end"
	time python3 run_gsd_ingest_threads.py  -s ${HOME}/VxIngest/test/load_spec_gsd-stations-v01.yaml -c ${credentials} -f $start -l $end

	echo "time python3 run_gsd_ingest_threads.py  -s ${HOME}/VxIngest/test/load_spec_gsd-metars-v01.yaml -c ${credentials} -f $start -l $end"
	time python3 run_gsd_ingest_threads.py  -s ${HOME}/VxIngest/test/load_spec_gsd-metars-v01.yaml -c ${credentials} -f $start -l $end

	echo "time python3 run_gsd_ingest_threads.py  -s ${HOME}/VxIngest/test/load_spec_gsd-hrrr-v01.yaml -c ${credentials} -f $start -l $end"
	time python3 run_gsd_ingest_threads.py  -s ${HOME}/VxIngest/test/load_spec_gsd-hrrr-v01.yaml -c ${credentials} -f $start -l $end

	echo "time python3 run_gsd_ingest_threads.py  -s ${HOME}/VxIngest/test/load_spec_gsd-hrrr_ops-v01.yaml -c ${credentials} -f $start -l $end"
	time python3 run_gsd_ingest_threads.py  -s ${HOME}/VxIngest/test/load_spec_gsd-hrrr_ops-v01.yaml -c ${credentials} -f $start -l $end

	echo "time python3 run_gsd_ingest_threads.py  -s ${HOME}/VxIngest/test/load_spec_gsd-rap_ops-v01.yaml -c ${credentials} -f $start -l $end"
	time python3 run_gsd_ingest_threads.py  -s ${HOME}/VxIngest/test/load_spec_gsd-rap_ops-v01.yaml -c ${credentials} -f $start -l $end

	echo "time python3 run_gsd_ingest_threads.py  -s ${HOME}/VxIngest/test/load_spec_gsd-rrfs_dev1-v01.yaml -c ${credentials} -f $start -l $end"
	time python3 run_gsd_ingest_threads.py  -s ${HOME}/VxIngest/test/load_spec_gsd-rrfs_dev1-v01.yaml -c ${credentials} -f $start -l $end

done
