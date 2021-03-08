#!/bin/sh
# find the max obs time in the couchbase
start=$(${HOME}/VxIngest/scripts/VXingest_utilities/find_cb_max_obs_epoch.sh ${HOME}/adb-cb4-credentials)
if [ $start == "null" ]; then
	# find the max obs time in the gsd mysql database
	echo Using minimum time from gsd mysql database
	start=$(${HOME}/VxIngest/scripts/VXingest_utilities/find_gsd_min_obs_epoch.sh ${HOME}/wolphin.cnf)
fi
echo start is $start
# find the max obs time in the gsd mysql database
stop=$(${HOME}/VxIngest/scripts/VXingest_utilities/find_gsd_max_obs_epoch.sh ${HOME}/wolphin.cnf)
echo stop is $stop
week=604800
end=$((start+week))
export PYTHONPATH=${HOME}/VxIngest
while [ $end -lt $stop ]; do
	echo "$start   $end"
	start=$(( $start + $week ))
	end=$(( $end +$week ))

	echo "time python3 run_gsd_ingest_threads.py  -s ${HOME}/VxIngest/test/load_spec_gsd-stations-v01.yaml -c ${HOME}/adb-cb4-credentials -f $start -l $end"
	#sleep 30
	#time python3 run_gsd_ingest_threads.py  -s ${HOME}/VxIngest/test/load_spec_gsd-stations-v01.yaml -c ${HOME}/adb-cb4-credentials -f $start -l $end

	echo "time python3 run_gsd_ingest_threads.py  -s ${HOME}/VxIngest/test/load_spec_gsd-metars-v01.yaml -c ${HOME}/adb-cb4-credentials -f $start -l $end"
	#sleep 30
	#time python3 run_gsd_ingest_threads.py  -s ${HOME}/VxIngest/test/load_spec_gsd-metars-v01.yaml -c ${HOME}/adb-cb4-credentials -f $start -l $end

	#echo "time python3 run_gsd_ingest_threads.py  -s ${HOME}/VxIngest/test/load_spec_gsd-hrrr-v01.yaml -c ${HOME}/adb-cb4-credentials -f $start -l $end"
	#sleep 30
	#time python3 run_gsd_ingest_threads.py  -s ${HOME}/VxIngest/test/load_spec_gsd-hrrr-v01.yaml -c ${HOME}/adb-cb4-credentials -f $start -l $end
done
