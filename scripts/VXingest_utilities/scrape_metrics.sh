#!/usr/bin/env bash


function is_epoch_rational {
    if [ -z "$1" ]; then
        echo "is_epoch_rational: ERROR: no epoch specified"
        usage
    fi

    if [ $1 -lt $(($(date +%s)-3600*24)) ]; then
        echo "is_epoch_rational: ERROR: irrational epoch - prior to yesterday at this time"
        usage
    fi

    if [ $1 -gt $(date +%s) ]; then
        echo "is_epoch_rational: ERROR: irrational epoch - beyond current time"
        usage
    fi
return 0
}

function derive_pattern_from_ids {
    # Determine an sql string pattern using wildcards that will be usable
    # in an sql query with a like statement.
    # Given a list of similar ids (same number of fields) this routine will
    # find the fields (':'seperated) that are not common
    # throught the list and substitute those fields with a "%25 (special char for %)" and return
    # that pattern.
    ids=("$@")
    # get the number of columns in these ingest_ids - just use the first one as they should all be the same
    num_columns=$(echo ${ids[0]} | awk -F ":" '{print NF}')
    differing_columns=()
    # find the columns that do not match for all the ingest ids - create an array of the differing column numbers
    for i in $(seq 1 $num_columns)
        do
            if [[ $(printf "%s\n" "${ids[@]}" | awk -F":" -vvar=$i '{print $var}' | awk '{$1=$1};1' | sort | uniq | wc -l) -ne 1 ]]
            then
                differing_columns+=( $i )
            fi
        done
    # now that we have the differing columns use the first id and replace the differing_columns with "%25 (special char for %)"
    pattern=${ids[0]}
    for i in ${differing_columns[@]}
    do
        pattern=$(echo $pattern | awk -F ":" -vfield=$i 'BEGIN { OFS=":" }{$field="%25"; print}')
    done
    echo ${pattern}
}

function get_record_count_from_log(){
    log_file=$1
    num_docs=0
    num_docs=$(grep "adding document DD" $log_file | sort | uniq | wc -l)
    echo $num_docs
}

function usage {
  echo "Usage $0 -c credentials-file -i import_log_file -l log_file -d textfile directory"
  echo "The credentials-file specifies cb_host, cb_user, and cb_password."
  echo "Metrics will be written into the textfile directory (-d)"
  echo "The scrape_metrics.sh script scans the log_file for the intended_record_count, and any errors,"
  echo "and queries the database to determine how many records were just added."
  exit 1
}

while getopts 'c:l:i:d:' param; do
  case "${param}" in
  c)
    credentials_file=${OPTARG}
    if [[ ! -f "${credentials_file}" ]]; then
      echo "${credentials_file} does not exist"
      usage
    fi
    cb_host=$(grep cb_host ${credentials_file} | awk '{print $2}')
    # if it is a multinode host split on ',' and take the first one
    IFS=','
    read -ra hostarr <<< "$cb_host"
    cb_host=${hostarr[0]}
    cb_user=$(grep cb_user ${credentials_file} | awk '{print $2}')
    cb_pwd=$(grep cb_password ${credentials_file} | awk '{print $2}')
    bucket=$(grep cb_bucket ${credentials_file} | awk '{print $2}')
    collection=$(grep cb_collection ${credentials_file} | awk '{print $2}')
    scope=$(grep cb_scope ${credentials_file} | awk '{print $2}')
    cred="${cb_user}:${cb_pwd}"
    ;;
  l)
    log_file=${OPTARG}
    if [[ ! -f "${log_file}" ]]; then
      echo "ERROR: log file ${log_file} does not exist"
      usage
    fi
    ;;
  i)
    import_log_file=${OPTARG}
    if [[ ! -f "${import_log_file}" ]]; then
      echo "ERROR: import log file ${import_log_file} does not exist"
      usage
    fi
    ;;
  d)
    textfile_dir=${OPTARG}
    if [[ ! -d "${textfile_dir}" ]]; then
      echo "ERROR: text file directory ${textfile_dir} does not exist"
      usage
    fi
    ;;
  *)
    echo "ERROR: wrong parameter, I don't do ${param}"
    usage
    ;;
  esac
done

if [ -z "${textfile_dir}" ]; then
    echo "ERROR: no textfile_dir specified"
    usage
fi

metric_name=$(grep 'metric_name' ${log_file} | awk '{print $6}')
start_epoch=$(date -d "$(grep  'Begin a_time:' ${log_file} | awk '{print $7" "$8}')" +"%s")
is_epoch_rational ${start_epoch}

finish_epoch=$(date -d "$(grep  'End a_time:' ${log_file} | awk '{print $7" "$8}')" +"%s")
is_epoch_rational ${finish_epoch}

#Get the error count from the log file
error_count=$(grep -i error ${log_file} | wc -l)

#Get the exit code from the log file
exit_code=$(grep exit_code ${log_file} | cut -d':' -f5)

# get the list of data document ids by greping "adding document DD:" from the log and awking the 5th param
# and determine the common pattern
dids=()
IFS=$'\r\n' dids=($(grep 'adding document DD:' ${log_file} | grep 'DD:' | sort | uniq | awk  '{print $9}'))
document_id_pattern=$(derive_pattern_from_ids "${dids[@]}")
# do not know how to do that yet, perhaps from the prior metrics - actual_duration_seconds?
expected_duration_seconds=0
actual_duration_seconds=$((finish_epoch - start_epoch))
error_count=${error_count}

# the cas meta field in couchbase is going to reflect the time that a document was imported, not when it was created.
# We need to get the start and stop epochs from the corresponding import log
start_import_epoch=$(grep Start ${import_log_file} | awk '{print $2}')
finish_import_epoch=$(grep Stop ${import_log_file} | awk '{print $2}')
# add 60 seconds for latency?
finish_import_epoch=$((finish_import_epoch + 60))
intended_record_count=$(get_record_count_from_log "${log_file}")
# NOTE: curl URL's don't like '%' or ';' characters. replace them with '%25' and '%3B' respectively (you can leave the ';' at the end of the statement off, actually)
echo "start_import_epoch is ${start_import_epoch}"
echo "finish_import_epoch is ${finish_import_epoch}"
echo "document_id_pattern is ${document_id_pattern}"
echo "log_file is ${log_file}"
if [[ -z $start_import_epoch ]] || [[ -z $finish_import_epoch ]]; then
	# there wasn't any start or finish time in the import - no records to import
	recorded_record_count=0
else
	recorded_record_count=$(curl -s http://${cb_host}:8093/query/service -u"${cred}" -d "statement=select raw count(meta().id) from ${bucket}.${scope}.${collection}  where CEIL(meta().cas / 1000000000) BETWEEN ${start_import_epoch} AND ${finish_import_epoch} AND meta().id like \"${document_id_pattern}\"" | jq -r '.results | .[]')
fi
tmp_metric_file=/tmp/${metric_name}_$$
record_count_difference=$((recorded_record_count - intended_record_count))
metric_file=${textfile_dir}/${metric_name}.prom
metric_name=$(echo "${metric_name}" | tr '[:upper:]' '[:lower:]')

# example metric name 'job_v01_metar_grib2_model_hrrr_adb_cb1'

# for getting historical data from promql...
# promql is a promql-cli tool that can be used to query prometheus
# it can be found at https://github.com/nalbury/promql-cli/releases
which promql > /dev/null
if [ $? -ne 0 ]
then
	echo "no promql in path"
	echo "promql is a promql-cli tool that can be used to query prometheus"
	echo "promql can be found at https://github.com/nalbury/promql-cli/releases"
	usage
fi
# we have to default these to 0 if they do not exist in the promql database - otherwise the scrape will fail next time
min_recorded_record_count_average=$(promql --no-headers --host "http://${cb_host}:9090"  "floor(min(avg_over_time({__name__=~'$metric_name',ingest_id=~'ingest_recorded_record_count'}[6h:1h])))" | awk '{print $1}')
if [[ "x" == "x${min_recorded_record_count_average}" ]] ; then
      min_recorded_record_count_average=0
fi
max_recorded_record_count_average=$(promql --no-headers --host "http://${cb_host}:9090"  "ceil(max(avg_over_time({__name__=~'$metric_name',ingest_id=~'ingest_recorded_record_count'}[6h:1h])))" | awk '{print $1}')
if [[ "x" == "x${max_recorded_record_count_average}" ]] ; then
      max_recorded_record_count_average=0
fi

min_actual_duration_seconds_average=$(promql --no-headers --host "http://${cb_host}:9090"  "floor(min(avg_over_time({__name__=~'$metric_name',ingest_id=~'ingest_actual_duration_seconds'}[6h:1h])))" | awk '{print $1}')
if [[ "x" == "x${min_actual_duration_seconds_average}" ]] ; then
      min_actual_duration_seconds_average=0
fi
max_actual_duration_seconds_average=$(promql --no-headers --host "http://${cb_host}:9090"  "ceil(max(avg_over_time({__name__=~'$metric_name',ingest_id=~'ingest_actual_duration_seconds'}[6h:1h])))" | awk '{print $1}')
if [[ "x" == "x${max_actual_duration_seconds_average}" ]] ; then
      max_actual_duration_seconds_average=0
fi

echo "${metric_name}{ingest_id=\"ingest_run_time\",log_file=\"${log_file}\",start_epoch=\"${start_epoch}\",stop_epoch=\"${finish_epoch}\"} 1" >> ${tmp_metric_file}

echo "${metric_name}{ingest_id=\"ingest_min_actual_duration_average_seconds\",log_file=\"${log_file}\"} ${min_actual_duration_seconds_average}" >> ${tmp_metric_file}
echo "${metric_name}{ingest_id=\"ingest_max_actual_duration_average_seconds\",log_file=\"${log_file}\"} ${max_actual_duration_seconds_average}" >> ${tmp_metric_file}
echo "${metric_name}{ingest_id=\"ingest_actual_duration_seconds\",log_file=\"${log_file}\"} ${actual_duration_seconds}" >> ${tmp_metric_file}

echo "${metric_name}{ingest_id=\"ingest_error_count\",log_file=\"${log_file}\"} ${error_count}" >> ${tmp_metric_file}

echo "${metric_name}{ingest_id=\"ingest_min_recorded_record_count_average\",log_file=\"${log_file}\"} ${min_recorded_record_count_average}" >> ${tmp_metric_file}
echo "${metric_name}{ingest_id=\"ingest_max_recorded_record_count_average\",log_file=\"${log_file}\"} ${max_recorded_record_count_average}" >> ${tmp_metric_file}
echo "${metric_name}{ingest_id=\"ingest_recorded_record_count\",log_file=\"${log_file}\"} ${recorded_record_count}" >> ${tmp_metric_file}

echo "${metric_name}{ingest_id=\"ingest_record_count_difference\",log_file=\"${log_file}\"} ${record_count_difference}" >> ${tmp_metric_file}

echo "${metric_name}{ingest_id=\"ingest_exit_code\",log_file=\"${log_file}\"} ${exit_code}" >> ${tmp_metric_file}

mv ${tmp_metric_file} ${metric_file}

