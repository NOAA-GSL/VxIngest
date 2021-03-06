# find the max obs time in the couchbase
start=$(./find_cb_max_obs_epoch.sh ~/adb-cb4-credentials)
echo start is $start
# find the max obs time in the gsd mysql database
stop=$(./find_gsd_max_obs_epoch.sh /Users/randy.pierce/wolphin.cnf)
echo stop is $stop
week=604800
end=$((start+week))
export PYTHONPATH=/home/pierce/VxIngest
while [ $end -lt $stop ]; do
	echo "$start   $end"
	start=$(( $start + $week ))
	end=$(( $end +$week ))
	echo "time python3 run_gsd_ingest_threads.py  -s /home/pierce/VxIngest/test/load_spec_gsd-station-v01.yaml -c ~/adb-cb4-credentials -f $start -l $end"
	echo time python3 run_gsd_ingest_threads.py  -s /home/pierce/VxIngest/test/load_spec_gsd-stations-v01.yaml -c ~/adb-cb4-credentials -f $start -l $end
done

